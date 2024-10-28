"""
Feature Table Cache service
"""

from __future__ import annotations

import asyncio
import os
from typing import Any, List, Optional

from bson import ObjectId
from redis import Redis
from redis.exceptions import LockNotOwnedError

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.logging import get_logger
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_table_cache_metadata import (
    CachedDefinitionWithTable,
    CachedFeatureDefinition,
    FeatureTableCacheMetadataModel,
)
from featurebyte.persistent.base import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.feature_table_cache_metadata import FeatureTableCacheMetadataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.storage import Storage

logger = get_logger(__name__)

FEATUREBYTE_FEATURE_TABLE_CACHE_MAX_COLUMNS = int(
    os.getenv("FEATUREBYTE_FEATURE_TABLE_CACHE_MAX_COLUMNS", "1000")
)
REDIS_LOCK_TIMEOUT = 240  # a maximum life for the lock in seconds


class FeatureTableCacheMetadataService(
    BaseDocumentService[
        FeatureTableCacheMetadataModel,
        FeatureTableCacheMetadataModel,
        FeatureTableCacheMetadataUpdate,
    ],
):
    """
    Feature Table Cache Metadata service
    """

    document_class = FeatureTableCacheMetadataModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        observation_table_service: ObservationTableService,
        storage: Storage,
        redis: Redis[Any],
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
            storage=storage,
            redis=redis,
        )
        self.observation_table_service = observation_table_service

    async def get_cached_definitions(
        self, observation_table_id: PydanticObjectId
    ) -> List[CachedDefinitionWithTable]:
        """
        Get cached feature definitions for observation table.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id

        Returns
        -------
        List[CachedFeatureDefinition]
            Cached feature definitions
        """
        cached_definitions = []
        async for cache_metadata in self.list_documents_iterator(
            query_filter={"observation_table_id": observation_table_id}
        ):
            for feature in cache_metadata.feature_definitions:
                cached_definitions.append(
                    CachedDefinitionWithTable(
                        feature_id=feature.feature_id,
                        definition_hash=feature.definition_hash,
                        feature_name=feature.feature_name,
                        table_name=cache_metadata.table_name,
                    )
                )
        return cached_definitions

    async def get_or_create_feature_table_cache(
        self,
        observation_table_id: PydanticObjectId,
        num_columns_to_insert: int,
    ) -> FeatureTableCacheMetadataModel:
        """Get or create feature table cache document for observation table.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id
        num_columns_to_insert: int
            Number of columns to insert

        Returns
        -------
        FeatureTableCacheMetadataModel
            Feature Table Cache model
        """
        while True:
            logger.info("Trying to acquire lock...")
            lock = self.redis.lock(
                f"get_or_create_feature_table_cache:{observation_table_id}",
                timeout=REDIS_LOCK_TIMEOUT,
            )
            if lock.acquire(blocking=False):
                try:
                    logger.info("Got lock!")
                    return await self._get_or_create_feature_table_cache(
                        observation_table_id, num_columns_to_insert
                    )
                finally:
                    # Release lock
                    if lock.owned():
                        try:
                            lock.release()
                        except LockNotOwnedError:
                            # Lock may have expired before release
                            pass
            await asyncio.sleep(0.1)

    async def _get_or_create_feature_table_cache(
        self,
        observation_table_id: PydanticObjectId,
        num_columns_to_insert: int,
    ) -> FeatureTableCacheMetadataModel:
        logger.info("----> In _get_or_create_feature_table_cache")
        query_filter = {"observation_table_id": observation_table_id}

        eligible_cache_metadata = None
        num_cache_tables = 0
        async for cache_metadata in self.list_documents_iterator(query_filter=query_filter):
            if (
                len(cache_metadata.feature_definitions) + num_columns_to_insert
            ) <= FEATUREBYTE_FEATURE_TABLE_CACHE_MAX_COLUMNS:
                eligible_cache_metadata = cache_metadata
            num_cache_tables += 1

        logger.info(f"----> In _get_or_create_feature_table_cache: {eligible_cache_metadata}")
        if eligible_cache_metadata is None:
            observation_table = await self.observation_table_service.get_document(
                document_id=observation_table_id
            )
            document = FeatureTableCacheMetadataModel(
                observation_table_id=observation_table.id,
                table_name=self._get_feature_cache_table_name(
                    observation_table_id, num_cache_tables
                ),
                feature_definitions=[],
            )
            eligible_cache_metadata = await self.create_document(document)

        logger.info("----> In _get_or_create_feature_table_cache, returning")
        return eligible_cache_metadata

    @staticmethod
    def _get_feature_cache_table_name(
        observation_table_id: PydanticObjectId,
        num_cache_tables: int,
    ) -> str:
        suffix = num_cache_tables + 1
        return f"{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_{str(observation_table_id)}_{suffix}"

    async def update_feature_table_cache(
        self,
        cache_metadata_id: PydanticObjectId,
        feature_definitions: List[CachedFeatureDefinition],
    ) -> None:
        """
        Update Feature Table Cache by adding new feature definitions.

        Parameters
        ----------
        cache_metadata_id: PydanticObjectId
            FeatureTableCacheMetadataModel identifier
        feature_definitions: List[CachedFeatureDefinition]
            Feature definitions
        """
        document = await self.get_document(cache_metadata_id)
        existing_features = {feat.definition_hash: feat for feat in document.feature_definitions}

        for feature in feature_definitions:
            if feature.definition_hash not in existing_features:
                existing_features[feature.definition_hash] = feature
            else:
                existing = existing_features[feature.definition_hash]
                if existing.feature_id is None:
                    existing_features[feature.definition_hash] = feature

        await self.update_document(
            document_id=document.id,
            data=FeatureTableCacheMetadataUpdate(
                feature_definitions=list(existing_features.values())
            ),
            return_document=False,
        )
