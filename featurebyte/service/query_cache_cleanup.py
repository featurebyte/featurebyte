"""
QueryCacheCleanupService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.logging import get_logger
from featurebyte.models.query_cache import QueryCacheType
from featurebyte.service.query_cache import QueryCacheDocumentService
from featurebyte.service.query_cache_cleanup_scheduler import QueryCacheCleanupSchedulerService
from featurebyte.service.session_helper import SessionHelper
from featurebyte.storage import Storage

logger = get_logger(__name__)


class QueryCacheCleanupService:
    """
    QueryCacheCleanupService is responsible for cleaning up stale query cache
    """

    def __init__(
        self,
        session_helper: SessionHelper,
        query_cache_document_service: QueryCacheDocumentService,
        query_cache_cleanup_scheduler_service: QueryCacheCleanupSchedulerService,
        storage: Storage,
    ):
        self.session_helper = session_helper
        self.query_cache_document_service = query_cache_document_service
        self.query_cache_cleanup_scheduler_service = query_cache_cleanup_scheduler_service
        self.storage = storage

    async def run_cleanup(self, feature_store_id: ObjectId) -> None:
        """
        Run cleanup on the query cache

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store identifier
        """
        fs_and_session = await self.session_helper.try_to_get_feature_store_and_session(
            feature_store_id=feature_store_id
        )
        if not fs_and_session:
            return

        _, session = fs_and_session
        logger.info("Query cache cleanup started for feature store %s", feature_store_id)
        n_success = 0
        n_failure = 0

        async for (
            cache_model
        ) in self.query_cache_document_service.list_stale_documents_as_dict_iterator(
            feature_store_id
        ):
            cached_object = cache_model["cached_object"]
            try:
                if cached_object["type"] == QueryCacheType.TEMP_TABLE:
                    await session.drop_table(
                        table_name=cached_object["table_name"],
                        schema_name=session.schema_name,
                        database_name=session.database_name,
                    )
                elif cached_object["type"] == QueryCacheType.DATAFRAME:
                    await self.storage.delete(cached_object["storage_path"])
                await self.query_cache_document_service.delete_document(cache_model["_id"])
            except Exception as e:
                n_failure += 1
                logger.exception(
                    "Error occurred while cleaning up query cache %s: %s",
                    cache_model["_id"],
                    e,
                )
                continue
            n_success += 1

        logger.info(
            "Query cache cleanup completed for feature store %s. %s documents cleaned up, %s failed",
            feature_store_id,
            n_success,
            n_failure,
        )
        await self.query_cache_cleanup_scheduler_service.stop_job_if_no_longer_needed(
            feature_store_id
        )
