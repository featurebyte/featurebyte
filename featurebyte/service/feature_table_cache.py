"""
Feature Table Cache service
"""
from typing import Any, List, Optional

from bson import ObjectId

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_table_cache import CachedFeatureDefinition, FeatureTableCacheModel
from featurebyte.persistent.base import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.schema.feature_table_cache import FeatureTableCacheUpdate
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
        document = await self.get_document_for_observation_table(observation_table_id)
        if document:
            existing_features = {
                feat.definition_hash: feat for feat in document.feature_definitions
            }
            for feature in feature_definitions:
                if feature.definition_hash not in existing_features:
                    existing_features[feature.definition_hash] = feature
                else:
                    existing = existing_features[feature.definition_hash]
                    if existing.feature_id is None:
                        existing_features[feature.definition_hash] = feature

            await self.update_document(
                document_id=document.id,
                data=FeatureTableCacheUpdate(feature_definitions=list(existing_features.values())),
                return_document=False,
            )
        else:
            observation_table = await self.observation_table_service.get_document(
                document_id=observation_table_id
            )
            document = FeatureTableCacheModel(
                observation_table_id=observation_table_id,
                table_name=f"feature_table_cache_{str(observation_table.id)}",
                feature_definitions=feature_definitions,
            )
            await self.create_document(document)
