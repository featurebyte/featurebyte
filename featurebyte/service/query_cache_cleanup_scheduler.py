"""
QueryCacheCleanupSchedulerService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.periodic_task import Interval
from featurebyte.persistent import DuplicateDocumentError
from featurebyte.schema.worker.task.query_cache_cleanup import QueryCacheCleanupTaskPayload
from featurebyte.service.query_cache import QueryCacheDocumentService
from featurebyte.service.task_manager import TaskManager

logger = get_logger(__name__)

QUERY_CACHE_CLEANUP_INTERVAL_SECONDS = 86400


class QueryCacheCleanupSchedulerService:
    """
    QueryCacheCleanupSchedulerService class
    """

    def __init__(
        self,
        user: User,
        task_manager: TaskManager,
        query_cache_document_service: QueryCacheDocumentService,
    ):
        self.user = user
        self.task_manager = task_manager
        self.task_manager.catalog_id = DEFAULT_CATALOG_ID
        self.query_cache_document_service = query_cache_document_service

    async def start_job_if_not_exist(self, feature_store_id: ObjectId) -> None:
        """
        Schedule a query cache cleanup job for a feature store if it does not exist

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id
        """
        job_id = self._get_job_id(feature_store_id)
        if await self.task_manager.get_periodic_task_by_name(name=job_id) is not None:
            return
        payload = QueryCacheCleanupTaskPayload(
            user_id=self.user.id,
            catalog_id=DEFAULT_CATALOG_ID,
            feature_store_id=feature_store_id,
        )
        logger.info(
            "Scheduling query cache cleanup job", extra={"feature_store_id": feature_store_id}
        )
        try:
            await self.task_manager.schedule_interval_task(
                name=job_id,
                payload=payload,
                interval=Interval(every=QUERY_CACHE_CLEANUP_INTERVAL_SECONDS, period="seconds"),
            )
        except DuplicateDocumentError:
            logger.warning(
                "Duplicated query cache cleanup task when scheduling",
                extra={"task_name": job_id},
            )

    async def stop_job_if_no_longer_needed(self, feature_store_id: ObjectId) -> None:
        """
        Stop a query cache cleanup job for a feature store if no longer needed (no more cached
        queries for the feature store)

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store id
        """
        docs = await self.query_cache_document_service.list_documents_as_dict(
            query_filter={"feature_store_id": feature_store_id},
        )
        if docs["data"]:
            return
        job_id = self._get_job_id(feature_store_id)
        await self.task_manager.delete_periodic_task_by_name(name=job_id)

    @classmethod
    def _get_job_id(cls, feature_store_id: ObjectId) -> str:
        return f"query_cache_cleanup_feature_store_id_{feature_store_id}"
