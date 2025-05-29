"""
FeatureMaterializeSyncService class
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import List, Optional

from bson import ObjectId
from pydantic import TypeAdapter

from featurebyte.common.date_util import get_current_job_datetime
from featurebyte.logging import get_logger
from featurebyte.models.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisite,
    PrerequisiteTileTask,
    PrerequisiteTileTaskStatusType,
)
from featurebyte.models.feature_materialize_run import FeatureMaterializeRun, IncompleteTileTask
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.query_graph.model.feature_job_setting import (
    FeatureJobSetting,
    FeatureJobSettingUnion,
)
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)
from featurebyte.service.cron_helper import CronHelper
from featurebyte.service.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisiteService,
)
from featurebyte.service.feature_materialize_run import FeatureMaterializeRunService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)

POLL_PERIOD_SECONDS = 5


def get_allowed_waiting_time_seconds(feature_job_setting: FeatureJobSetting) -> int:
    """
    Get the amount of time allowed to wait for prequisites

    Parameters
    ----------
    feature_job_setting: FeatureJobSetting
        Feature job setting

    Returns
    -------
    int
    """
    return feature_job_setting.period_seconds // 2


class FeatureMaterializeSyncService:
    """
    Service to manage synchronization between tile tasks and feature materialize tasks
    """

    def __init__(
        self,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feature_materialize_prerequisite_service: FeatureMaterializePrerequisiteService,
        feature_materialize_run_service: FeatureMaterializeRunService,
        task_manager: TaskManager,
    ):
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feature_materialize_prerequisite_service = feature_materialize_prerequisite_service
        self.feature_materialize_run_service = feature_materialize_run_service
        self.task_manager = task_manager

    async def initialize_prerequisite(
        self, offline_store_feature_table_id: ObjectId
    ) -> Optional[FeatureMaterializePrerequisite]:
        """
        Create a FeatureMaterializePrerequisite document for a feature store table at the current
        job cycle

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table id

        Returns
        -------
        Optional[FeatureMaterializePrerequisite]
        """
        current_job_datetime = await self._get_scheduled_job_ts_for_feature_table(
            offline_store_feature_table_id=offline_store_feature_table_id
        )
        if current_job_datetime is None:
            return None
        document = (
            await self.feature_materialize_prerequisite_service.get_or_create_for_feature_table(
                offline_store_feature_table_id=offline_store_feature_table_id,
                scheduled_job_ts=current_job_datetime,
            )
        )
        return document

    async def update_tile_prerequisite(
        self,
        tile_task_ts: datetime,
        aggregation_ids: list[str],
        status: PrerequisiteTileTaskStatusType,
    ) -> None:
        """
        Update feature materialize prerequisite.

        To be called by a tile task at the end of its execution. This can update prerequisites for
        multiple offline store feature tables.

        Parameters
        ----------
        tile_task_ts: datetime
            Start time of the tile task
        aggregation_ids: list[str]
            Aggregation ids corresponding to the tile task
        status: PrerequisiteTileTaskStatusType
            Status of the tile task
        """
        logger.info(
            "Updating tile prerequisite",
            extra={
                "tile_tasks_ts": str(tile_task_ts),
                "aggregation_ids": aggregation_ids,
                "status": status,
            },
        )
        async for (
            feature_table_model
        ) in self.offline_store_feature_table_service.list_feature_tables_for_aggregation_ids(
            aggregation_ids
        ):
            if feature_table_model.feature_job_setting is None:
                continue
            schedule_job_datetime = self._get_scheduled_job_ts_from_datetime(
                input_dt=tile_task_ts,
                feature_job_setting=feature_table_model.feature_job_setting,
            )
            # Only update the prerequisite for aggregation ids that are part of the feature table
            common_aggregation_ids = set(aggregation_ids).intersection(
                feature_table_model.aggregation_ids
            )
            prerequisite_tile_tasks = [
                PrerequisiteTileTask(
                    aggregation_id=aggregation_id,
                    status=status,
                )
                for aggregation_id in common_aggregation_ids
            ]
            await self.feature_materialize_prerequisite_service.add_completed_prerequisite(
                offline_store_feature_table_id=feature_table_model.id,
                scheduled_job_ts=schedule_job_datetime,
                prerequisite_tile_tasks=prerequisite_tile_tasks,
            )

    async def run_feature_materialize(self, offline_store_feature_table_id: ObjectId) -> None:
        """
        The entry point of all feature materialize tasks

        This will be called in a scheduled IO task. It will wait for all the prerequisites for a
        feature materialize task to be met before triggering the task, with a deadline.

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table id
        """
        feature_table = await self.offline_store_feature_table_service.get_document(
            offline_store_feature_table_id
        )
        feature_materialize_run = await self.feature_materialize_run_service.create_document(
            FeatureMaterializeRun(
                offline_store_feature_table_id=offline_store_feature_table_id,
                offline_store_feature_table_name=feature_table.name,
                scheduled_job_ts=await self._get_scheduled_job_ts_for_feature_table(
                    offline_store_feature_table_id,
                ),
                deployment_ids=feature_table.deployment_ids,
            ),
        )

        # No need to wait for feature tables without prerequisites
        if not feature_table.aggregation_ids:
            await self._submit_feature_materialize_task(feature_table, feature_materialize_run.id)
            return

        prerequisite = await self.initialize_prerequisite(offline_store_feature_table_id)
        assert prerequisite is not None

        feature_job_setting = feature_table.feature_job_setting
        assert feature_job_setting is not None
        assert isinstance(feature_job_setting, FeatureJobSetting)

        tic = prerequisite.scheduled_job_ts.timestamp()
        prerequisite_met = False
        logger.info(
            "Waiting for prerequisites for feature materialize task",
            extra={
                "offline_store_feature_table_id": feature_table.id,
                "scheduled_job_ts": str(prerequisite.scheduled_job_ts),
            },
        )
        allowed_waiting_time_seconds = get_allowed_waiting_time_seconds(feature_job_setting)
        while datetime.utcnow().timestamp() - tic < allowed_waiting_time_seconds:
            prerequisite_model = await self.feature_materialize_prerequisite_service.get_document(
                prerequisite.id
            )
            completed_aggregation_ids = [
                item.aggregation_id for item in prerequisite_model.completed
            ]
            if set(feature_table.aggregation_ids).issubset(set(completed_aggregation_ids)):
                logger.info(
                    "Prerequisites for feature materialize task met",
                    extra={"offline_store_feature_table_id": feature_table.id},
                )
                prerequisite_met = True
                break
            await asyncio.sleep(POLL_PERIOD_SECONDS)

        if not prerequisite_met:
            logger.warning(
                "Running feature materialize task but prerequisites are not met",
                extra={"offline_store_feature_table_id": feature_table.id},
            )
            prerequisite_model = await self.feature_materialize_prerequisite_service.get_document(
                prerequisite.id
            )
            incomplete_tile_tasks = self.get_incomplete_tile_tasks(
                feature_table_aggregation_ids=feature_table.aggregation_ids,
                prerequisite_model=prerequisite_model,
            )
            await self.feature_materialize_run_service.update_incomplete_tile_tasks(
                feature_materialize_run.id, incomplete_tile_tasks
            )

        await self._submit_feature_materialize_task(
            feature_table, feature_materialize_run_id=feature_materialize_run.id
        )

    @classmethod
    def get_incomplete_tile_tasks(
        cls,
        feature_table_aggregation_ids: List[str],
        prerequisite_model: FeatureMaterializePrerequisite,
    ) -> List[IncompleteTileTask]:
        """
        Get incomplete tile tasks

        Parameters
        ----------
        feature_table_aggregation_ids: List[str]
            Aggregation ids for the feature table
        prerequisite_model: FeatureMaterializePrerequisite
            Prerequisite model

        Returns
        -------
        List[IncompleteTileTask]
        """
        incomplete_tile_tasks = []
        completed_aggregation_ids = []
        for item in prerequisite_model.completed:
            if item.status == "failure":
                incomplete_tile_tasks.append(
                    IncompleteTileTask(aggregation_id=item.aggregation_id, reason="failure")
                )
            else:
                completed_aggregation_ids.append(item.aggregation_id)
        for aggregation_id in set(feature_table_aggregation_ids) - set(completed_aggregation_ids):
            incomplete_tile_tasks.append(
                IncompleteTileTask(aggregation_id=aggregation_id, reason="timeout")
            )
        return incomplete_tile_tasks

    async def _submit_feature_materialize_task(
        self,
        offline_store_feature_table: OfflineStoreFeatureTableModel,
        feature_materialize_run_id: ObjectId,
    ) -> None:
        payload = ScheduledFeatureMaterializeTaskPayload(
            offline_store_feature_table_name=offline_store_feature_table.name,
            offline_store_feature_table_id=offline_store_feature_table.id,
            feature_materialize_run_id=feature_materialize_run_id,
            catalog_id=self.offline_store_feature_table_service.catalog_id,
            user_id=self.offline_store_feature_table_service.user.id,
        )
        await self.task_manager.submit(payload, mark_as_scheduled_task=True)

    async def _get_scheduled_job_ts_for_feature_table(
        self, offline_store_feature_table_id: ObjectId
    ) -> Optional[datetime]:
        feature_table_dict = await self.offline_store_feature_table_service.get_document_as_dict(
            offline_store_feature_table_id,
            projection={"feature_job_setting": 1},
        )
        if feature_table_dict.get("feature_job_setting") is None:
            return None
        current_job_datetime = self._get_scheduled_job_ts_from_datetime(
            input_dt=datetime.utcnow(),
            feature_job_setting=TypeAdapter(FeatureJobSettingUnion).validate_python(
                feature_table_dict["feature_job_setting"]
            ),
        )
        return current_job_datetime

    @classmethod
    def _get_scheduled_job_ts_from_datetime(
        cls,
        input_dt: datetime,
        feature_job_setting: FeatureJobSettingUnion,
    ) -> datetime:
        if isinstance(feature_job_setting, FeatureJobSetting):
            return get_current_job_datetime(
                input_dt=input_dt,
                frequency_minutes=feature_job_setting.period_seconds // 60,
                time_modulo_frequency_seconds=feature_job_setting.offset_seconds,
            )

        return CronHelper.get_next_scheduled_job_ts(
            cron_feature_job_setting=feature_job_setting,
            current_ts=input_dt,
        )
