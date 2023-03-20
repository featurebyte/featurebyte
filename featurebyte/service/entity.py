"""
EntityService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.schema.entity import EntityCreate, EntityServiceUpdate
from featurebyte.service.base_document import BaseDocumentService


class EntityService(BaseDocumentService[EntityModel, EntityCreate, EntityServiceUpdate]):
    """
    EntityService class
    """

    document_class = EntityModel

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
        return [EntityModel(**doc) async for doc in docs]

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
        return [EntityModel(**doc) async for doc in docs]

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
        return [EntityModel(**doc) async for doc in docs]
