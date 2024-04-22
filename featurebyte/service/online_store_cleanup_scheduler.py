"""
OnlineStoreCleanupSchedulerService class
"""

from typing import Optional

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.periodic_task import Interval, PeriodicTask
from featurebyte.persistent import DuplicateDocumentError, Persistent
from featurebyte.schema.worker.task.online_store_cleanup import OnlineStoreCleanupTaskPayload
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)

CLEANUP_INTERVAL_SECONDS = 60 * 60 * 24  # Once per day


class OnlineStoreCleanupSchedulerService:
    """
    OnlineStoreCleanupSchedulerService is responsible for scheduling tasks to periodically clean up
    stale versions in online store tables. Each task will clean up a single online store table.
    """

    def __init__(
        self,
        persistent: Persistent,
        user: User,
        catalog_id: ObjectId,
        task_manager: TaskManager,
    ):
        self.user = user
        self.persistent = persistent
        self.catalog_id = catalog_id
        self.task_manager = task_manager

    async def start_job_if_not_exist(
        self, catalog_id: ObjectId, feature_store_id: ObjectId, online_store_table_name: str
    ) -> None:
        """
        Schedule the cleanup job if not already scheduled

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog id
        feature_store_id: ObjectId
            Feature store id
        online_store_table_name: str
            Name of the online store table to be cleaned up in the scheduled task.
        """
        payload = OnlineStoreCleanupTaskPayload(
            user_id=self.user.id,
            catalog_id=catalog_id,
            feature_store_id=feature_store_id,
            online_store_table_name=online_store_table_name,
        )
        if await self.get_periodic_task(online_store_table_name) is None:
            logger.info(
                "Scheduling online store cleanup job",
                extra={"online_store_table_name": online_store_table_name},
            )
            try:
                await self.task_manager.schedule_interval_task(
                    name=self._get_job_id(online_store_table_name),
                    payload=payload,
                    interval=Interval(every=CLEANUP_INTERVAL_SECONDS, period="seconds"),
                )
            except DuplicateDocumentError:
                logger.warning(
                    "Duplicated online store cleanup task",
                    extra={"task_name": self._get_job_id(online_store_table_name)},
                )
        else:
            logger.debug(
                "Online store cleanup job already exists",
                extra={"online_store_table_name": online_store_table_name},
            )

    async def stop_job(self, online_store_table_name: str) -> None:
        """
        Stop job

        Parameters
        ----------
        online_store_table_name: str
            Online store table name
        """
        job_id = self._get_job_id(online_store_table_name)
        await self.task_manager.delete_periodic_task_by_name(job_id)

    async def get_periodic_task(self, online_store_table_name: str) -> Optional[PeriodicTask]:
        """
        Get the periodic task corresponding to feature store id

        Parameters
        ----------
        online_store_table_name: str
            Online store table name

        Returns
        -------
        Optional[PeriodicTask]
        """
        return await self.task_manager.get_periodic_task_by_name(
            name=self._get_job_id(online_store_table_name)
        )

    def _get_job_id(self, online_store_table_name: str) -> str:
        return f"online_store_cleanup_{online_store_table_name}"
