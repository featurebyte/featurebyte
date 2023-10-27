"""
Entity Relationship Extractor Service
"""
from typing import Dict, List

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte.models.feature import EntityRelationshipInfo
from featurebyte.routes.common.derive_primary_entity_helper import DerivePrimaryEntityHelper
from featurebyte.service.entity import EntityService
from featurebyte.service.relationship_info import RelationshipInfoService


@dataclass
class ParentData:
    """
    Parent data
    """

    parent_entity_id: ObjectId
    relationship_id: ObjectId


class EntityRelationshipExtractorService:
    """
    Entity Relationship Extractor Service
    """

    def __init__(
        self,
        entity_service: EntityService,
        relationship_info_service: RelationshipInfoService,
        derive_primary_entity_helper: DerivePrimaryEntityHelper,
    ):
        self.entity_service = entity_service
        self.relationship_info_service = relationship_info_service
        self.derive_primary_entity_helper = derive_primary_entity_helper

    async def extract_all_relationships(
        self,
        entity_ids: List[ObjectId],
    ) -> List[EntityRelationshipInfo]:
        """
        Extract all relationships of the given entity IDs

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

        output = []
        async for relationship_info in self.relationship_info_service.list_documents_iterator(
            query_filter={
                "$or": [
                    {"entity_id": {"$in": list(descendant_entity_ids)}},
                    {"related_entity_id": {"$in": list(ancestor_entity_ids)}},
                ]
            }
        ):
            output.append(EntityRelationshipInfo(**relationship_info.dict(by_alias=True)))
        return output

    @classmethod
    def _extract_entity_paths(
        cls,
        parent_map: Dict[ObjectId, List[ParentData]],
        entity_id: ObjectId,
        path_map: Dict[ObjectId, List[ObjectId]],
        relationship_ids: List[ObjectId],
    ) -> Dict[ObjectId, List[ObjectId]]:
        path_map[entity_id] = relationship_ids
        for parent_data in parent_map[entity_id]:
            path_map = cls._extract_entity_paths(
                parent_map=parent_map,
                entity_id=parent_data.parent_entity_id,
                path_map=path_map,
                relationship_ids=relationship_ids + [parent_data.relationship_id],
            )

        return path_map

    async def extract_relationship_from_primary_entity(
        self,
        entity_ids: List[ObjectId],
    ) -> List[EntityRelationshipInfo]:
        """
        Extract relationships between entity ids and primary entity ids

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity ids

        Returns
        -------
        List[EntityRelationshipInfo]
        """
        primary_entity_ids = await self.derive_primary_entity_helper.derive_primary_entity_ids(
            entity_ids=entity_ids
        )

        ancestor_entity_ids = set()
        async for entity in self.entity_service.list_documents_iterator(
            query_filter={"_id": {"$in": primary_entity_ids}}
        ):
            ancestor_entity_ids.update(entity.ancestor_ids)

        # construct mapping {child_entity_id => parent_data(parent_entity_id, relationship_id)}
        parent_map: Dict[ObjectId, List[ParentData]] = defaultdict(list)
        async for relationship_info in self.relationship_info_service.list_documents_iterator(
            query_filter={"related_entity_id": {"$in": list(ancestor_entity_ids)}},
        ):
            parent_data = ParentData(
                parent_entity_id=relationship_info.related_entity_id,
                relationship_id=relationship_info.id,
            )
            parent_map[relationship_info.entity_id].append(parent_data)

        # construct mapping {entity_id => relationship_ids from primary entity to entity}
        path_map: Dict[ObjectId, List[ObjectId]] = defaultdict(list)
        for entity_id in primary_entity_ids:
            path_map = self._extract_entity_paths(
                parent_map=parent_map, entity_id=entity_id, path_map=path_map, relationship_ids=[]
            )

        # get all required relationship ids
        relationship_ids = set()
        for entity_id in entity_ids:
            relationship_ids.update(path_map.get(entity_id, []))

        # get all relationship info
        output = []
        async for relationship_info in self.relationship_info_service.list_documents_iterator(
            query_filter={"_id": {"$in": list(relationship_ids)}},
        ):
            output.append(EntityRelationshipInfo(**relationship_info.dict(by_alias=True)))
        return output
