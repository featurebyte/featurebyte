"""
FeatureStoreTableCleanupSchedulerService class
"""

from typing import Optional

from bson import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.periodic_task import Interval, PeriodicTask
from featurebyte.persistent import DuplicateDocumentError, Persistent
from featurebyte.schema.worker.task.feature_store_table_cleanup import (
    FeatureStoreTableCleanupTaskPayload,
)
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)

CLEANUP_INTERVAL_SECONDS = 60 * 60 * 24  # Every 24 hours


class FeatureStoreTableCleanupSchedulerService:
    """
    FeatureStoreTableCleanupSchedulerService is responsible for scheduling tasks to periodically
    clean up expired warehouse tables. Each task will clean up all expired tables for a single feature store.
    """

    def __init__(
        self,
        persistent: Persistent,
        user: User,
        task_manager: TaskManager,
    ):
        self.user = user
        self.persistent = persistent
        self.task_manager = task_manager

    async def start_job_if_not_exist(self, feature_store_id: ObjectId) -> None:
        """
        Schedule the cleanup job if not already scheduled

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id
        """
        # Note: catalog_id is required by BaseTaskPayload but the cleanup task operates
        # across all catalogs for the feature store. We use DEFAULT_CATALOG_ID if none provided.
        payload = FeatureStoreTableCleanupTaskPayload(
            user_id=self.user.id,
            catalog_id=DEFAULT_CATALOG_ID,
            feature_store_id=feature_store_id,
        )
        if await self.get_periodic_task(feature_store_id) is None:
            logger.info(
                "Scheduling feature store table cleanup job",
                extra={"feature_store_id": str(feature_store_id)},
            )
            try:
                await self.task_manager.schedule_interval_task(
                    name=self._get_job_id(feature_store_id),
                    payload=payload,
                    interval=Interval(every=CLEANUP_INTERVAL_SECONDS, period="seconds"),
                )
            except DuplicateDocumentError:
                logger.warning(
                    "Duplicated feature store table cleanup task",
                    extra={"task_name": self._get_job_id(feature_store_id)},
                )
        else:
            logger.debug(
                "Feature store table cleanup job already exists",
                extra={"feature_store_id": str(feature_store_id)},
            )

    async def stop_job(self, feature_store_id: ObjectId) -> None:
        """
        Stop job

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id
        """
        job_id = self._get_job_id(feature_store_id)
        await self.task_manager.delete_periodic_task_by_name(job_id)

    async def get_periodic_task(self, feature_store_id: ObjectId) -> Optional[PeriodicTask]:
        """
        Get the periodic task corresponding to feature store id

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id

        Returns
        -------
        Optional[PeriodicTask]
        """
        return await self.task_manager.get_periodic_task_by_name(
            name=self._get_job_id(feature_store_id)
        )

    def _get_job_id(self, feature_store_id: ObjectId) -> str:
        return f"feature_store_table_cleanup_{feature_store_id}"
