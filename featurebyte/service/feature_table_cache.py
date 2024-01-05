"""
Feature Table Cache service
"""
from typing import Any, List, Optional

import hashlib

from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_table_cache import FeatureTableCacheModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.feature_table_cache import (
    CachedFeatureDefinition,
    FeatureTableCacheInfo,
    FeatureTableCacheUpdate,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.observation_table import ObservationTableService


class FeatureTableCacheService(
    BaseDocumentService[FeatureTableCacheModel, FeatureTableCacheModel, FeatureTableCacheUpdate],
):
    """
    Feature Table Cache metadata service
    """

    document_class = FeatureTableCacheModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        observation_table_service: ObservationTableService,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
        )
        self.observation_table_service = observation_table_service

    async def get_document_for_observation_table(
        self,
        observation_table_id: PydanticObjectId,
    ) -> Optional[FeatureTableCacheModel]:
        """Get document for observation table.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id

        Returns
        -------
        Optional[FeatureTableCacheModel]
            Feature Table Cache model if exists
        """
        documents = []

        query_filter = {"observation_table_id": observation_table_id}
        async for document in self.list_documents_iterator(query_filter=query_filter):
            documents.append(document)

        if not documents:
            return None

        return documents[0]

    async def get_feature_table_cache_info(
        self,
        observation_table_id: PydanticObjectId,
    ) -> Optional[FeatureTableCacheInfo]:
        """Get document for observation table.

        Parameters
        ----------
        observation_table_id: PydanticObjectId
            Observation table id

        Returns
        -------
        Optional[FeatureTableCacheInfo]
            Feature Table Cache Info
        """
        document = await self.get_document_for_observation_table(observation_table_id)
        if not document:
            return None

        return FeatureTableCacheInfo(
            cache_table_name=document.name,
            cached_feature_names=document.features,
        )

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
        features = []
        for feature_def in feature_definitions:
            feature_name = feature_def.name
            if feature_def.definition_hash:
                feature_name = f"{feature_name}_{feature_def.definition_hash}"
            features.append(feature_name)

        document = await self.get_document_for_observation_table(observation_table_id)
        if document:
            updated_features = document.features.copy()
            for feature in features:
                if feature not in updated_features:
                    updated_features.append(feature)

            await self.update_document(
                document_id=document.id,
                data=FeatureTableCacheUpdate(features=updated_features),
                return_document=False,
            )
        else:
            observation_table = await self.observation_table_service.get_document(
                document_id=observation_table_id
            )

            entity_ids = []
            if observation_table.primary_entity_ids:
                entity_ids = [str(entity_id) for entity_id in observation_table.primary_entity_ids]

            hasher = hashlib.shake_128()
            name = f"{observation_table.id}_{','.join(sorted(entity_ids))}".encode("utf-8")
            hasher.update(name)
            name_hash = hasher.hexdigest(20)

            document = FeatureTableCacheModel(
                observation_table_id=observation_table_id,
                name=f"feature_table_cache_{name_hash}",
                features=features,
            )
            await self.create_document(document)
