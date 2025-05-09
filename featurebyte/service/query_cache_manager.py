"""
QueryCacheManagerService
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow
from bson import ObjectId
from sqlglot.expressions import Select

from featurebyte.logging import get_logger, truncate_query
from featurebyte.models.query_cache import (
    CachedDataFrame,
    CachedTable,
    QueryCacheModel,
    QueryCacheType,
)
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.service.query_cache import QueryCacheDocumentService
from featurebyte.service.query_cache_cleanup_scheduler import QueryCacheCleanupSchedulerService
from featurebyte.session.base import BaseSession
from featurebyte.storage import Storage

logger = get_logger(__name__)


class QueryCacheManagerService:
    """
    QueryCacheService class
    """

    def __init__(
        self,
        query_cache_document_service: QueryCacheDocumentService,
        query_cache_cleanup_scheduler_service: QueryCacheCleanupSchedulerService,
        storage: Storage,
    ):
        self.query_cache_document_service = query_cache_document_service
        self.query_cache_cleanup_scheduler_service = query_cache_cleanup_scheduler_service
        self.storage = storage

    async def get_or_cache_table(
        self,
        session: BaseSession,
        feature_store_id: ObjectId,
        table_expr: Select,
    ) -> str:
        """
        Get a table from the cache corresponding to table_expr if available. Otherwise, create the
        table and cache it.

        Parameters
        ----------
        session: BaseSession
            Database session
        feature_store_id: ObjectId
            Feature store identifier
        table_expr: Select
            Table expression of the table to be retrieved or created

        Returns
        -------
        str
            Name of the table
        """
        query = sql_to_string(table_expr, source_type=session.source_type)

        def _get_table_details(_table_name: str) -> TableDetails:
            return TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=_table_name,
            )

        cached_table_name = await self.get_cached_table(feature_store_id, query)

        # Check validity of the cached table
        if cached_table_name is not None:
            if not await session.table_exists(_get_table_details(cached_table_name)):
                logger.warning(
                    "Cached table does not exist", extra={"table_name": cached_table_name}
                )
                cached_table_name = None

        if cached_table_name is None:
            table_name = f"__FB_CACHED_TABLE_{ObjectId()}".upper()
            logger.info(
                "Caching table for query",
                extra={"table_name": cached_table_name, "query": truncate_query(query)},
            )
            await session.create_table_as(
                table_details=_get_table_details(table_name), select_expr=table_expr
            )
            await self.cache_table(feature_store_id, query, table_name)
            return table_name

        logger.info(
            "Using cached table for query",
            extra={"table_name": cached_table_name, "query": truncate_query(query)},
        )
        return cached_table_name

    async def get_or_cache_dataframe(
        self,
        session: BaseSession,
        feature_store_id: ObjectId,
        query: str,
        allow_long_running: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Get a dataframe from the cache corresponding to query if available. Otherwise, execute the
        query and cache the resulting dataframe.

        Parameters
        ----------
        session: BaseSession
            Database session
        feature_store_id: ObjectId
            Feature store identifier
        query: str
            Query to be executed if it is not cached
        allow_long_running: bool
            Allow long running query execution

        Returns
        -------
        Optional[pd.DataFrame]
        """
        cached_df = await self.get_cached_dataframe(feature_store_id, query)
        if cached_df is None:
            logger.info("Caching dataframe for query", extra={"query": truncate_query(query)})
            result_df = await self._execute_query(session, query, allow_long_running)
            if result_df is not None:
                await self.cache_dataframe(feature_store_id, query, result_df)
            return result_df
        logger.info("Using cached dataframe for query", extra={"query": truncate_query(query)})
        return cached_df

    async def get_cached_table(self, feature_store_id: ObjectId, query: str) -> Optional[str]:
        """
        Get the name of the cached table corresponding to the query if available

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store identifier
        query: str
            Query to lookup

        Returns
        -------
        Optional[str]
            Name of the cached table
        """
        key = self._get_cache_key(feature_store_id, query, QueryCacheType.TEMP_TABLE)
        cache_model = await self._get_document_by_key(key)
        if cache_model is None:
            return None
        assert isinstance(cache_model.cached_object, CachedTable)
        return cache_model.cached_object.table_name

    async def cache_table(self, feature_store_id: ObjectId, query: str, table_name: str) -> None:
        """
        Cache the table corresponding to the query

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store identifier
        query: str
            Query to cache
        table_name: str
            Name of the table
        """
        key = self._get_cache_key(feature_store_id, query, QueryCacheType.TEMP_TABLE)
        cache_model = QueryCacheModel(
            feature_store_id=feature_store_id,
            query=query,
            cache_key=key,
            cached_object=CachedTable(table_name=table_name),
        )
        await self.query_cache_document_service.create_document(cache_model)
        await self.query_cache_cleanup_scheduler_service.start_job_if_not_exist(feature_store_id)

    async def get_cached_dataframe(
        self, feature_store_id: ObjectId, query: str
    ) -> Optional[pd.DataFrame]:
        """
        Get the cached dataframe corresponding to the query if available

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store identifier
        query: str
            Query to lookup

        Returns
        -------
        Optional[pd.DataFrame]
            Cached dataframe
        """
        # Check if the query is cached
        key = self._get_cache_key(feature_store_id, query, QueryCacheType.DATAFRAME)
        cache_model = await self._get_document_by_key(key)
        if cache_model is None:
            return None

        # Download the cached dataframe
        cached_object = cache_model.cached_object
        assert isinstance(cached_object, CachedDataFrame)
        try:
            return await self.storage.get_dataframe(Path(cached_object.storage_path))
        except FileNotFoundError:
            logger.warning(
                "Cached dataframe does not exist",
                extra={"query": truncate_query(query)},
            )
            return None

    async def cache_dataframe(
        self, feature_store_id: ObjectId, query: str, dataframe: pd.DataFrame
    ) -> None:
        """
        Cache the dataframe corresponding to the query

        Parameters
        ----------
        feature_store_id: ObjectId
            Feature store identifier
        query: str
            Query to cache
        dataframe: pd.DataFrame
            Result of executing the query
        """
        cache_key = self._get_cache_key(feature_store_id, query, QueryCacheType.DATAFRAME)

        # Upload to storage
        path = f"query_cache/{feature_store_id}/{ObjectId()}.parquet"
        try:
            await self.storage.put_dataframe(dataframe, Path(path))
        except pyarrow.ArrowException:
            logger.warning(
                "Failed to cache dataframe due to unsupported data type",
                extra={"query": truncate_query(query)},
            )
            return

        # Create cache model record
        cache_model = QueryCacheModel(
            feature_store_id=feature_store_id,
            query=query,
            cache_key=cache_key,
            cached_object=CachedDataFrame(storage_path=path),
        )
        await self.query_cache_document_service.create_document(cache_model)
        await self.query_cache_cleanup_scheduler_service.start_job_if_not_exist(feature_store_id)

    async def _get_document_by_key(self, key: str) -> Optional[QueryCacheModel]:
        query_filter = {"cache_key": key}
        query_filter.update(self.query_cache_document_service.get_cache_validity_filter())
        async for doc in self.query_cache_document_service.list_documents_iterator(
            query_filter=query_filter
        ):
            return doc
        return None

    @classmethod
    def _get_cache_key(
        cls, feature_store_id: ObjectId, query: str, cache_type: QueryCacheType
    ) -> str:
        cache_key = ":".join([query, str(feature_store_id), cache_type.value])
        cache_key = hashlib.new("md5", cache_key.encode("utf-8"), usedforsecurity=False).hexdigest()
        return cache_key

    @classmethod
    async def _execute_query(
        cls, session: BaseSession, query: str, allow_long_running: bool
    ) -> Optional[pd.DataFrame]:
        if allow_long_running:
            result = await session.execute_query_long_running(query)
        else:
            result = await session.execute_query(query)
        return result
