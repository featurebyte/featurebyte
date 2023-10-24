"""
Entity Relationship Extractor
"""
from typing import List

from bson import ObjectId

from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship_info import RelationshipInfoService


class EntityRelationshipExtractorService:
    """
    Entity Relationship Extractor Service
    """

    def __init__(
        self,
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
    ):
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service

    async def extract(self, entity_ids: List[ObjectId]) -> List[EntityRelationshipInfo]:
        """
        Extract entity relationships for a list of entity IDs

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity IDs

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        ancestor_entity_ids = set(entity_ids)
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            ancestor_entity_ids.update(entity.ancestor_ids)

        descendant_entity_ids = set(entity_ids)
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"ancestor_ids": {"$in": list(entity_ids)}}
        ):
            descendant_entity_ids.add(entity.id)

        relationships_info = [
            EntityRelationshipInfo(**relationship_info)
            async for relationship_info in self.relationship_info_service.list_documents_as_dict_iterator(
                query_filter={
                    "$or": [
                        {"entity_id": {"$in": list(descendant_entity_ids)}},
                        {"related_entity_id": {"$in": list(ancestor_entity_ids)}},
                    ]
                }
            )
        ]
        return relationships_info
