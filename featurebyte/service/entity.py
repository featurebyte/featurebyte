"""
EntityService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.persistent import Persistent
from featurebyte.routes.block_modification_handler import BlockModificationHandler
from featurebyte.routes.catalog.catalog_name_injector import CatalogNameInjector
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate
from featurebyte.schema.info import EntityBriefInfoList
from featurebyte.service.base_document import BaseDocumentService


class EntityService(BaseDocumentService[EntityModel, EntityCreate, EntityServiceUpdate]):
    """
    EntityService class
    """

    document_class = EntityModel

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: Optional[ObjectId],
        catalog_name_injector: CatalogNameInjector,
        block_modification_handler: BlockModificationHandler,
    ):
        super().__init__(user, persistent, catalog_id, block_modification_handler)
        self.catalog_name_injector = catalog_name_injector

    @staticmethod
    def _extract_additional_creation_kwargs(data: EntityCreate) -> dict[str, Any]:
        return {"serving_names": [data.serving_name]}

    async def get_entities_with_serving_names(
        self, serving_names_set: set[str]
    ) -> list[EntityModel]:
        """
        Retrieve all entities matching the set of provided serving names

        Parameters
        ----------
        serving_names_set: set[str]
            Set of serving names to match

        Returns
        -------
        list[EntityModel]
        """
        docs = self.list_documents_iterator(
            query_filter={"serving_names": {"$in": list(serving_names_set)}}
        )
        return [doc async for doc in docs]

    async def get_children_entities(self, entity_id: ObjectId) -> list[EntityModel]:
        """
        Retrieve the children of an entity

        Parameters
        ----------
        entity_id: ObjectId
            Entity identifier

        Returns
        -------
        list[EntityModel]
        """
        query_filter = {"parents": {"$elemMatch": {"id": ObjectId(entity_id)}}}
        docs = self.list_documents_iterator(query_filter=query_filter)
        return [doc async for doc in docs]

    async def get_entities(self, entity_ids: set[ObjectId]) -> list[EntityModel]:
        """
        Retrieve entities given a list of entity ids

        Parameters
        ----------
        entity_ids: list[ObjectId]
            Entity identifiers

        Returns
        -------
        list[EntityModel]
        """
        docs = self.list_documents_iterator(query_filter={"_id": {"$in": list(entity_ids)}})
        return [doc async for doc in docs]

    async def get_entity_brief_info_list(self, entity_ids: set[ObjectId]) -> EntityBriefInfoList:
        """
        Retrieve entities given a list of entity ids

        Parameters
        ----------
        entity_ids: set[ObjectId]
            Entity identifiers

        Returns
        -------
        EntityBriefInfoList
        """
        entities = await self.list_documents_as_dict(
            page=1, page_size=0, query_filter={"_id": {"$in": list(entity_ids)}}
        )
        if not entities:
            return EntityBriefInfoList(data=[])
        # Get catalog ID
        catalog_id = entities["data"][0]["catalog_id"]
        _, updated_docs = await self.catalog_name_injector.add_name(catalog_id, [entities])
        return EntityBriefInfoList.from_paginated_data(updated_docs[0])
