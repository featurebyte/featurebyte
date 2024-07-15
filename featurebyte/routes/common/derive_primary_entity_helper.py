"""
Derive primary entity helper
"""

from __future__ import annotations

from typing import Any, Optional, Sequence

from bson import ObjectId

from featurebyte.models.entity import EntityModel
from featurebyte.models.relationship_analysis import derive_primary_entity
from featurebyte.service.entity import EntityService


class DerivePrimaryEntityHelper:
    """Mixin class to derive primary entity from a list of entities"""

    def __init__(self, entity_service: EntityService):
        self.entity_service = entity_service

    async def get_entity_id_to_entity(
        self, doc_list: list[dict[str, Any]]
    ) -> dict[ObjectId, EntityModel]:
        """
        Construct entity ID to entity dictionary mapping

        Parameters
        ----------
        doc_list: list[dict[str, Any]]
            List of document dictionary (document should contain entity_ids field)

        Returns
        -------
        dict[ObjectId, EntityModel]
            Dictionary mapping entity ID to entity model
        """
        entity_ids = set()
        for doc in doc_list:
            entity_ids.update(doc["entity_ids"])

        entity_id_to_entity: dict[ObjectId, EntityModel] = {}
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            entity_id_to_entity[entity.id] = entity
        return entity_id_to_entity

    async def derive_primary_entity_ids(
        self,
        entity_ids: Sequence[ObjectId],
        entity_id_to_entity: Optional[dict[ObjectId, EntityModel]] = None,
    ) -> list[ObjectId]:
        """
        Derive primary entity IDs from a list of entity IDs

        Parameters
        ----------
        entity_ids: Sequence[ObjectId]
            List of entity IDs
        entity_id_to_entity: Optional[dict[ObjectId, EntityModel]]
            Dictionary mapping entity ID to entity dictionary

        Returns
        -------
        list[ObjectId]
        """
        if entity_id_to_entity is None:
            entity_id_to_entity = {
                entity.id: entity
                async for entity in self.entity_service.list_documents_iterator(
                    query_filter={"_id": {"$in": entity_ids}},
                )
            }

        entities = [entity_id_to_entity[entity_id] for entity_id in entity_ids]
        return [entity.id for entity in derive_primary_entity(entities=entities)]
