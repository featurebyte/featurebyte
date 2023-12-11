"""
Feast registry service
"""
from typing import Any, Optional, cast

from bson import ObjectId

from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.schema.registry import FeastRegistryCreate, FeastRegistryUpdate
from featurebyte.feast.utils.registry_construction import FeastRegistryConstructor
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_store import FeatureStoreService


class FeastRegistryService(
    BaseDocumentService[FeastRegistryModel, FeastRegistryCreate, FeastRegistryUpdate]
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
        catalog_service: CatalogService,
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
        self.catalog_service = catalog_service

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
        if not feature_store_ids:
            assert self.catalog_id is not None
            catalog = await self.catalog_service.get_document(document_id=self.catalog_id)
            assert len(catalog.default_feature_store_ids) > 0
            feature_store_ids.add(catalog.default_feature_store_ids[0])

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
        data = await self._construct_feast_registry_model(data=data)  # type: ignore
        return await super().create_document(data=data)

    async def update_document(
        self,
        document_id: ObjectId,
        data: FeastRegistryUpdate,
        exclude_none: bool = True,
        document: Optional[FeastRegistryModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
    ) -> Optional[FeastRegistryModel]:
        assert data.registry is None, "Registry will be generated automatically from feature lists"
        assert data.feature_store_id is None, "Not allowed to update feature store ID directly"

        original_doc = await self.get_document(document_id=document_id)
        recreated_model = await self._construct_feast_registry_model(
            data=FeastRegistryCreate(
                project_name=original_doc.name,
                feature_lists=data.feature_lists,
            )
        )
        updated = await super().update_document(
            document_id=document_id,
            data=FeastRegistryUpdate(
                registry=recreated_model.registry,
                feature_store_id=recreated_model.feature_store_id,
                feature_lists=None,
            ),
            exclude_none=exclude_none,
            document=document,
            return_document=return_document,
            skip_block_modification_check=skip_block_modification_check,
        )
        return cast(FeastRegistryModel, updated)

    async def get_feast_registry_for_catalog(self) -> Optional[FeastRegistryModel]:
        """
        Get feast registry document for the catalog if it exists

        Returns
        -------
        Optional[FeastRegistryModel]
        """
        async for feast_registry_model in self.list_documents_iterator(
            query_filter={"catalog_id": self.catalog_id}
        ):
            return feast_registry_model
        return None
