"""
FeatureMaterializeSchedulerService class
"""

from typing import Optional

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.offline_store_feature_table import OfflineStoreFeatureTableModel
from featurebyte.models.periodic_task import Crontab, Interval, PeriodicTask
from featurebyte.persistent import DuplicateDocumentError
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.schema.worker.task.feature_materialize_sync import (
    FeatureMaterializeSyncTaskPayload,
)
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)


class FeatureMaterializeSchedulerService:
    """
    FeatureMaterializeSchedulerService is responsible for scheduling tasks to periodically
    materialize features into offline store feature tables.
    """

    def __init__(
        self,
        user: User,
        catalog_id: ObjectId,
        task_manager: TaskManager,
    ):
        self.user = user
        self.catalog_id = catalog_id
        self.task_manager = task_manager

    async def start_job_if_not_exist(
        self,
        offline_store_feature_table: OfflineStoreFeatureTableModel,
    ) -> None:
        """
        Schedule the feature materialize job if not already scheduled

        Parameters
        ----------
        offline_store_feature_table: OfflineStoreFeatureTableModel
            Offline store feature table
        """
        await self._stop_deprecated_job(offline_store_feature_table.id)
        payload = FeatureMaterializeSyncTaskPayload(
            user_id=self.user.id,
            catalog_id=self.catalog_id,
            offline_store_feature_table_name=offline_store_feature_table.name,
            offline_store_feature_table_id=offline_store_feature_table.id,
        )
        if await self.get_periodic_task(offline_store_feature_table.id) is None:
            logger.info(
                "Scheduling feature materialize job",
                extra={"offline_store_feature_table": offline_store_feature_table.name},
            )
            try:
                if offline_store_feature_table.feature_job_setting is None:
                    return
                if isinstance(offline_store_feature_table.feature_job_setting, FeatureJobSetting):
                    await self.task_manager.schedule_interval_task(
                        name=self._get_job_id(offline_store_feature_table.id),
                        payload=payload,
                        interval=Interval(
                            every=offline_store_feature_table.feature_job_setting.period_seconds,
                            period="seconds",
                        ),
                        time_modulo_frequency_second=offline_store_feature_table.feature_job_setting.offset_seconds,
                    )
                else:
                    crontab = offline_store_feature_table.feature_job_setting.crontab
                    assert isinstance(crontab, Crontab)
                    await self.task_manager.schedule_cron_task(
                        name=self._get_job_id(offline_store_feature_table.id),
                        payload=payload,
                        crontab=crontab,
                        timezone=offline_store_feature_table.feature_job_setting.timezone,
                    )
            except DuplicateDocumentError:
                logger.warning(
                    "Duplicate feature materialize job",
                    extra={"task_name": self._get_job_id(offline_store_feature_table.id)},
                )
        else:
            logger.info(
                "Feature materialize job already scheduled",
                extra={"offline_store_feature_table": offline_store_feature_table.name},
            )

    async def stop_job(self, offline_store_feature_table_id: ObjectId) -> None:
        """
        Stop job

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table id
        """
        await self._stop_deprecated_job(offline_store_feature_table_id)
        job_id = self._get_job_id(offline_store_feature_table_id)
        await self.task_manager.delete_periodic_task_by_name(job_id)

    async def get_periodic_task(
        self, offline_store_feature_table_id: ObjectId
    ) -> Optional[PeriodicTask]:
        """
        Get the periodic task corresponding to offline store feature table id

        Parameters
        ----------
        offline_store_feature_table_id: ObjectId
            Offline store feature table id

        Returns
        -------
        Optional[PeriodicTask]
        """
        return await self.task_manager.get_periodic_task_by_name(
            self._get_job_id(offline_store_feature_table_id)
        )

    @classmethod
    def _get_job_id(cls, offline_store_feature_table_id: ObjectId) -> str:
        return f"feature_materialize_sync_{offline_store_feature_table_id}"

    @classmethod
    def _get_deprecated_job_id(cls, offline_store_feature_table_id: ObjectId) -> str:
        # For backward compatibility. Before we schedule SCHEDULED_FEATURE_MATERIALIZE task
        # directly, but now we schedule FEATURE_MATERIALIZE_SYNC. Can be removed once all
        # deprecated jobs are stopped.
        return f"scheduled_feature_materialize_{offline_store_feature_table_id}"

    async def _stop_deprecated_job(self, offline_store_feature_table_id: ObjectId) -> None:
        job_id = self._get_deprecated_job_id(offline_store_feature_table_id)
        await self.task_manager.delete_periodic_task_by_name(job_id)
