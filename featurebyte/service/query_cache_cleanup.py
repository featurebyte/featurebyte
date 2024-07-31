"""
QueryCacheCleanupService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.models.query_cache import QueryCacheType
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.query_cache import QueryCacheDocumentService
from featurebyte.service.query_cache_cleanup_scheduler import QueryCacheCleanupSchedulerService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.storage import Storage


class QueryCacheCleanupService:
    """
    QueryCacheCleanupService is responsible for cleaning up stale query cache
    """

    def __init__(
        self,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        query_cache_document_service: QueryCacheDocumentService,
        query_cache_cleanup_scheduler_service: QueryCacheCleanupSchedulerService,
        storage: Storage,
    ):
        self.session_manager_service = session_manager_service
        self.feature_store_service = feature_store_service
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
        feature_store = await self.feature_store_service.get_document(feature_store_id)
        session = await self.session_manager_service.get_feature_store_session(feature_store)
        async for (
            cache_model
        ) in self.query_cache_document_service.list_stale_documents_as_dict_iterator(
            feature_store_id
        ):
            await self.query_cache_document_service.delete_document(cache_model["_id"])
            cached_object = cache_model["cached_object"]
            if cached_object["type"] == QueryCacheType.TEMP_TABLE:
                await session.drop_table(
                    table_name=cached_object["table_name"],
                    schema_name=session.schema_name,
                    database_name=session.database_name,
                )
            elif cached_object["type"] == QueryCacheType.DATAFRAME:
                await self.storage.delete(cached_object["storage_path"])

        await self.query_cache_cleanup_scheduler_service.stop_job_if_no_longer_needed(
            feature_store_id
        )
