"""
Feast registry service
"""
from __future__ import annotations

from typing import Any, List, Optional

import random
from pathlib import Path

from bson import ObjectId
from redis import Redis
from redis.lock import Lock

from featurebyte.feast.model.registry import FeastRegistryModel
from featurebyte.feast.schema.registry import FeastRegistryCreate, FeastRegistryUpdate
from featurebyte.feast.utils.registry_construction import FeastRegistryBuilder
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.catalog import CatalogService
from featurebyte.service.entity import EntityService
from featurebyte.service.entity_lookup_feature_table import EntityLookupFeatureTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_store import OnlineStoreService
from featurebyte.storage import Storage

FEAST_REGISTRY_REDIS_LOCK_TIMEOUT = 120  # a maximum life for the lock in seconds


class FeastRegistryService(
    BaseDocumentService[FeastRegistryModel, FeastRegistryCreate, FeastRegistryUpdate]
):
    """Feast registry service"""

    document_class = FeastRegistryModel

    def __init__(  # pylint: disable=too-many-arguments
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        block_modification_handler: BlockModificationHandler,
        feature_list_service: FeatureListService,
        feature_service: FeatureService,
        entity_service: EntityService,
        feature_store_service: FeatureStoreService,
        online_store_service: OnlineStoreService,
        catalog_service: CatalogService,
        entity_lookup_feature_table_service: EntityLookupFeatureTableService,
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
        self.feature_list_service = feature_list_service
        self.feature_service = feature_service
        self.entity_service = entity_service
        self.feature_store_service = feature_store_service
        self.online_store_service = online_store_service
        self.catalog_service = catalog_service
        self.entity_lookup_feature_table_service = entity_lookup_feature_table_service

    def get_registry_storage_lock(self, timeout: int) -> Lock:
        """
        Get registry storage lock

        Parameters
        ----------
        timeout: int
            Maximum life for the lock in seconds

        Returns
        -------
        Lock
        """
        return self.redis.lock(f"feast_registry_storage_update:{self.catalog_id}", timeout=timeout)

    async def _create_project_name(
        self, catalog_id: ObjectId, hex_digit_num: int = 7, max_try: int = 100
    ) -> str:
        # generate 7 hex digits
        project_name = str(catalog_id)[-hex_digit_num:]
        document_dict = await self.persistent.find_one(
            collection_name=self.collection_name,
            query_filter={"name": project_name},
            projection={"_id": 1},
        )
        if not document_dict:
            return project_name

        count = 0
        while True:
            project_name = f"{random.randrange(16**hex_digit_num):0{hex_digit_num}x}"
            document_dict = await self.persistent.find_one(
                collection_name=self.collection_name,
                query_filter={"name": project_name},
                projection={"_id": 1},
            )
            if not document_dict:
                return project_name

            count += 1
            if count > max_try:
                raise RuntimeError("Unable to generate unique project name")

    async def _create_offline_table_name_prefix(self, feature_store_id: ObjectId) -> str:
        res, _ = await self.persistent.aggregate_find(
            collection_name=self.collection_name,
            pipeline=[
                {"$match": {"feature_store_id": feature_store_id}},
                {"$group": {"_id": None, "unique_names": {"$addToSet": "$name"}}},
            ],
        )
        results = list(res)
        found_names = set(results[0]["unique_names"]) if results else set()
        name_count = len(found_names)
        return f"cat{name_count + 1}"

    async def get_or_create_feast_registry(
        self,
        catalog_id: ObjectId,
        feature_store_id: Optional[ObjectId],
    ) -> FeastRegistryModel:
        """
        Get or create project name

        Parameters
        ----------
        catalog_id: ObjectId
            Catalog id
        feature_store_id: Optional[ObjectId]
            Feature store id

        Returns
        -------
        FeastRegistryModel
        """
        query_filter = {"catalog_id": catalog_id}
        if feature_store_id:
            query_filter["feature_store_id"] = feature_store_id
        else:
            catalog = await self.catalog_service.get_document(document_id=catalog_id)
            query_filter["feature_store_id"] = catalog.default_feature_store_ids[0]

        query_result = await self.list_documents_as_dict(query_filter=query_filter, page_size=1)
        if query_result["total"]:
            registry = await self._populate_remote_attributes(
                FeastRegistryModel(**query_result["data"][0])
            )
            return registry

        registry = await self.create_document(data=FeastRegistryCreate(feature_lists=[]))
        return registry

    async def _construct_feast_registry_model(  # pylint: disable=too-many-locals
        self,
        project_name: Optional[str],
        offline_table_name_prefix: Optional[str],
        feature_lists: List[FeatureListModel],
        document_id: Optional[ObjectId] = None,
    ) -> FeastRegistryModel:
        # retrieve latest feature lists
        feature_ids = set()
        recent_feature_lists = []
        for feature_list in feature_lists:
            recent_feature_list = await self.feature_list_service.get_document(
                document_id=feature_list.id
            )
            recent_feature_lists.append(recent_feature_list)
            feature_ids.update(recent_feature_list.feature_ids)

        features = []
        entity_ids = set()
        feature_lists = recent_feature_lists
        feature_store_ids = set()
        async for feature in self.feature_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(feature_ids)}}
        ):
            features.append(feature)
            entity_ids.update(feature.entity_ids)
            feature_store_ids.add(feature.tabular_source.feature_store_id)

        entities = []
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entities.append(entity)

        if len(feature_store_ids) > 1:
            raise ValueError("Feature store IDs must be the same for all features")

        assert self.catalog_id is not None
        catalog = await self.catalog_service.get_document(document_id=self.catalog_id)
        if not feature_store_ids:
            assert len(catalog.default_feature_store_ids) > 0
            feature_store_ids.add(catalog.default_feature_store_ids[0])

        feature_store_id = feature_store_ids.pop()
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)

        online_store = None
        if catalog.online_store_id:
            online_store = await self.online_store_service.get_document(
                document_id=catalog.online_store_id
            )

        entity_lookup_steps_mapping = (
            await self.entity_lookup_feature_table_service.get_entity_lookup_steps_mapping(
                feature_lists
            )
        )

        if not project_name:
            project_name = await self._create_project_name(catalog_id=self.catalog_id)
        if not offline_table_name_prefix:
            offline_table_name_prefix = await self._create_offline_table_name_prefix(
                feature_store_id=feature_store_id
            )

        feast_registry_proto = FeastRegistryBuilder.create(
            feature_store=feature_store,
            online_store=online_store,
            entities=entities,
            features=features,
            feature_lists=feature_lists,
            project_name=project_name,
            entity_lookup_steps_mapping=entity_lookup_steps_mapping,
        )
        return FeastRegistryModel(
            _id=document_id,
            name=project_name,
            offline_table_name_prefix=offline_table_name_prefix,
            registry=feast_registry_proto.SerializeToString(),
            feature_store_id=feature_store_id,
        )

    async def _populate_remote_attributes(self, document: FeastRegistryModel) -> FeastRegistryModel:
        if document.registry_path:
            document.registry = await self.storage.get_bytes(Path(document.registry_path))
        return document

    async def _move_registry_to_storage(self, document: FeastRegistryModel) -> FeastRegistryModel:
        feast_registry_path = self.get_full_remote_file_path(
            f"feast_registry/{document.id}/feast_registry.pb"
        )
        await self.storage.put_bytes(document.registry, feast_registry_path)
        document.registry_path = str(feast_registry_path)
        document.registry = b""
        return document

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
        with self.get_registry_storage_lock(FEAST_REGISTRY_REDIS_LOCK_TIMEOUT):
            document = await self._construct_feast_registry_model(
                project_name=None, offline_table_name_prefix=None, feature_lists=data.feature_lists
            )
            document = await self._move_registry_to_storage(document)
            return await super().create_document(data=document)  # type: ignore

    async def update_document(
        self,
        document_id: ObjectId,
        data: FeastRegistryUpdate,
        exclude_none: bool = True,
        document: Optional[FeastRegistryModel] = None,
        return_document: bool = True,
        skip_block_modification_check: bool = False,
    ) -> Optional[FeastRegistryModel]:
        assert data.feature_store_id is None, "Not allowed to update feature store ID directly"
        if data.feature_lists is None:
            return await self.get_document(document_id=document_id)

        with self.get_registry_storage_lock(FEAST_REGISTRY_REDIS_LOCK_TIMEOUT):
            original_doc = await self.get_document(
                document_id=document_id, populate_remote_attributes=False
            )
            recreated_model = await self._construct_feast_registry_model(
                project_name=original_doc.name,
                offline_table_name_prefix=original_doc.offline_table_name_prefix,
                feature_lists=data.feature_lists,
                document_id=document_id,
            )
            assert recreated_model.id == document_id

            if original_doc.registry_path:
                # attempt to remove old registry file
                await self.storage.try_delete_if_exists(Path(original_doc.registry_path))

            document = await self._move_registry_to_storage(recreated_model)
            await self.persistent.update_one(
                collection_name=self.collection_name,
                query_filter=self._construct_get_query_filter(document_id=document.id),
                update={
                    "$set": {"registry_path": document.registry_path},
                    "$unset": {"registry": ""},  # remove registry field from older document
                },
                user_id=self.user.id,
                disable_audit=self.should_disable_audit,
            )
            return await self.get_document(document_id=document_id)

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
            return await self._populate_remote_attributes(feast_registry_model)
        return None
