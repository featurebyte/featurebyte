"""
Feast registry service
"""
from typing import Any, Optional

from bson import ObjectId

from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.schema.registry import FeastRegistryCreate
from featurebyte.feast.utils.registry_construction import FeastRegistryConstructor
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService


class FeastRegistryService(
    BaseDocumentService[FeastRegistryModel, FeastRegistryCreate, FeastRegistryCreate]
):
    """Feast registry service"""

    document_class = FeastRegistryModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        feature_service: FeatureService,
        entity_service: EntityService,
        feature_store_service: FeatureStoreService,
    ):
        super().__init__(
            user=user,
            persistent=persistent,
            catalog_id=catalog_id,
            block_modification_handler=block_modification_handler,
        )
        self.feature_service = feature_service
        self.entity_service = entity_service
        self.feature_store_service = feature_store_service

    async def _construct_feast_registry_model(
        self, data: FeastRegistryCreate
    ) -> FeastRegistryModel:
        feature_ids = set()
        for feature_list in data.feature_lists:
            feature_ids.update(feature_list.feature_ids)

        features = []
        entity_ids = set()
        feature_store_ids = set()
        async for feature in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(feature_ids)}}
        ):
            features.append(feature)
            entity_ids.update(feature.primary_entity_ids)
            feature_store_ids.add(feature.tabular_source.feature_store_id)

        entities = []
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entities.append(entity)

        if len(feature_store_ids) > 1:
            raise ValueError("Feature store IDs must be the same for all features")

        feature_store_id = feature_store_ids.pop()
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)

        feast_registry_proto = FeastRegistryConstructor.create(
            feature_store=feature_store,
            entities=entities,
            features=features,
            feature_lists=data.feature_lists,
            project_name=data.project_name,
        )
        return FeastRegistryModel(
            name=data.project_name,
            registry=feast_registry_proto.SerializeToString(),
            feature_store_id=feature_store_id,
        )

    async def create_document(self, data: FeastRegistryCreate) -> FeastRegistryModel:
        """
        Create document

        Parameters
        ----------
        data: FeastRegistryCreate
            Data to create document

        Returns
        -------
        FeastRegistryModel
            Created document
        """
        data = await self._construct_feast_registry_model(data=data)
        return await super().create_document(data=data)
