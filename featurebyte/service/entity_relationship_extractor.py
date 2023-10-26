"""
Entity Relationship Extractor Service
"""
from typing import Dict, List, Set, Tuple

from collections import defaultdict

from bson import ObjectId

from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.models.relationship import RelationshipInfoModel
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

    async def _extract_all_relationships(
        self, entity_ids: List[ObjectId]
    ) -> List[RelationshipInfoModel]:
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

        return [
            relationship_info
            async for relationship_info in self.relationship_info_service.list_documents_iterator(
                query_filter={
                    "$or": [
                        {"entity_id": {"$in": list(descendant_entity_ids)}},
                        {"related_entity_id": {"$in": list(ancestor_entity_ids)}},
                    ]
                }
            )
        ]

    @classmethod
    def depth_first_search(
        cls,
        relationship_map: Dict[ObjectId, List[ObjectId]],
        from_entity_id: ObjectId,
        from_first: bool,
    ) -> Set[Tuple[ObjectId, ObjectId]]:
        """
        Depth first search for entity relationships

        Parameters
        ----------
        relationship_map: Dict[ObjectId, List[ObjectId]]
            Relationship map
        from_entity_id: ObjectId
            From entity ID
        from_first: bool
            Keep the from entity ID first or not

        Returns
        -------
        Set[Tuple[ObjectId, ObjectId]]
        """
        output = set()
        for to_entity_id in relationship_map[from_entity_id]:
            entity_pair = (
                (from_entity_id, to_entity_id) if from_first else (to_entity_id, from_entity_id)
            )
            output.add(entity_pair)

            # keep searching
            output.update(
                cls.depth_first_search(
                    relationship_map=relationship_map,
                    from_entity_id=to_entity_id,
                    from_first=from_first,
                )
            )
        return output

    async def extract(
        self,
        entity_ids: List[ObjectId],
        keep_all_ancestors: bool = True,
        keep_all_descendants: bool = True,
    ) -> List[EntityRelationshipInfo]:
        """
        Extract entity relationships for a list of entity IDs

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity IDs
        keep_all_ancestors: bool
            Keep all ancestors
        keep_all_descendants: bool
            Keep all descendants

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        all_relationships_info = await self._extract_all_relationships(entity_ids=entity_ids)

        # construct entity relationship map
        ancestor_map: Dict[ObjectId, List[ObjectId]] = defaultdict(list)
        descendant_map: Dict[ObjectId, List[ObjectId]] = defaultdict(list)
        for relationship_info in all_relationships_info:
            ancestor_map[relationship_info.entity_id].append(relationship_info.related_entity_id)
            descendant_map[relationship_info.related_entity_id].append(relationship_info.entity_id)

        # depth first search for entity relationships
        entity_pairs = set()
        if keep_all_ancestors:
            for entity_id in entity_ids:
                entity_pairs.update(
                    self.depth_first_search(
                        relationship_map=ancestor_map, from_entity_id=entity_id, from_first=True
                    )
                )

        if keep_all_descendants:
            for entity_id in entity_ids:
                entity_pairs.update(
                    self.depth_first_search(
                        relationship_map=descendant_map, from_entity_id=entity_id, from_first=False
                    )
                )

        # filter out entity relationships that are not in the entity pairs
        output = []
        for relationship_info in all_relationships_info:
            key = (relationship_info.entity_id, relationship_info.related_entity_id)
            if key in entity_pairs:
                output.append(EntityRelationshipInfo(**relationship_info.dict(by_alias=True)))
        return output
