"""
FeatureMaterializeSyncService class
"""

from __future__ import annotations

from typing import Optional

from datetime import datetime

from bson import ObjectId

from featurebyte.common.date_util import get_current_job_datetime
from featurebyte.models.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisite,
    PrerequisiteTileTask,
    PrerequisiteTileTaskStatusType,
)
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.service.feature_materialize_prerequisite import (
    FeatureMaterializePrerequisiteService,
)
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService


class FeatureMaterializeSyncService:
    """
    Service to manage synchronization between tile tasks and feature materialize tasks
    """

    def __init__(
        self,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feature_materialize_prerequisite_service: FeatureMaterializePrerequisiteService,
    ):
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feature_materialize_prerequisite_service = feature_materialize_prerequisite_service

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
        """
        current_job_datetime = await self._get_scheduled_job_ts_for_feature_table(
            offline_store_feature_table_id=offline_store_feature_table_id
        )
        if current_job_datetime is None:
            return None
        feature_materialize_prerequisite = FeatureMaterializePrerequisite(
            offline_store_feature_table_id=offline_store_feature_table_id,
            scheduled_job_ts=current_job_datetime,
        )
        created_document = await self.feature_materialize_prerequisite_service.create_document(
            feature_materialize_prerequisite
        )
        return created_document

    async def update_tile_prerequisite(
        self,
        tile_task_ts: datetime,
        aggregation_id: str,
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
        aggregation_id: str
            Aggregation id corresponding to the tile task
        status: PrerequisiteTileTaskStatusType
            Status of the tile task
        """
        async for (
            feature_table_model
        ) in self.offline_store_feature_table_service.list_feature_tables_for_aggregation_id(
            aggregation_id
        ):
            if feature_table_model.feature_job_setting is None:
                continue
            schedule_job_datetime = self._get_scheduled_job_ts_from_datetime(
                input_dt=tile_task_ts,
                feature_job_setting=feature_table_model.feature_job_setting,
            )
            prerequisite_tile_task = PrerequisiteTileTask(
                aggregation_id=aggregation_id,
                status=status,
            )
            await self.feature_materialize_prerequisite_service.add_completed_prerequisite(
                offline_store_feature_table_id=feature_table_model.id,
                scheduled_job_ts=schedule_job_datetime,
                prerequisite_tile_task=prerequisite_tile_task,
            )

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
            feature_job_setting=FeatureJobSetting(**feature_table_dict["feature_job_setting"]),
        )
        return current_job_datetime

    @classmethod
    def _get_scheduled_job_ts_from_datetime(
        cls,
        input_dt: datetime,
        feature_job_setting: FeatureJobSetting,
    ) -> datetime:
        return get_current_job_datetime(
            input_dt=input_dt,
            frequency_minutes=feature_job_setting.period_seconds // 60,
            time_modulo_frequency_seconds=feature_job_setting.offset_seconds,
        )
