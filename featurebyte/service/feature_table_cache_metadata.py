"""
Feature Table Cache service
"""

from __future__ import annotations

from typing import Any, List, Optional

from bson import ObjectId
from redis import Redis

from featurebyte.enum import MaterializedTableNamePrefix
from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_table_cache_metadata import (
    CachedFeatureDefinition,
    FeatureTableCacheMetadataModel,
)
from featurebyte.persistent.base import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.feature_table_cache_metadata import FeatureTableCacheMetadataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.storage import Storage


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

    async def get_or_create_feature_table_cache(
        self,
        observation_table_id: PydanticObjectId,
    ) -> FeatureTableCacheMetadataModel:
        """Get or create feature table cache document for observation table.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id

        Returns
        -------
        FeatureTableCacheMetadataModel
            Feature Table Cache model
        """
        documents = []

        query_filter = {"observation_table_id": observation_table_id}
        async for document in self.list_documents_iterator(query_filter=query_filter):
            documents.append(document)

        if documents:
            document = documents[0]
        else:
            observation_table = await self.observation_table_service.get_document(
                document_id=observation_table_id
            )
            document = FeatureTableCacheMetadataModel(
                observation_table_id=observation_table.id,
                table_name=f"{MaterializedTableNamePrefix.FEATURE_TABLE_CACHE}_{str(observation_table.id)}",
                feature_definitions=[],
            )
            document = await self.create_document(document)

        return document

    async def update_feature_table_cache(
        self,
        observation_table_id: PydanticObjectId,
        feature_definitions: List[CachedFeatureDefinition],
    ) -> None:
        """
        Update Feature Table Cache by adding new feature definitions.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id
        feature_definitions: List[CachedFeatureDefinition]
            Feature definitions
        """
        document = await self.get_or_create_feature_table_cache(observation_table_id)
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
