"""
This module contains entity relationship info related classes.
"""
from typing import Dict, List, Set

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId
from pydantic import Field

from featurebyte.models.base import FeatureByteBaseModel, PydanticObjectId
from featurebyte.models.relationship import RelationshipType

# maximum depth to search for entity relationship, need to store this in feature if we want to change it
# in the future
ENTITY_RELATIONSHIP_MAX_DEPTH = 5


class EntityRelationshipInfo(FeatureByteBaseModel):
    """
    Schema for entity relationship information (subset of existing RelationshipInfo)
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id", allow_mutation=False)
    relationship_type: RelationshipType
    entity_id: PydanticObjectId
    related_entity_id: PydanticObjectId
    relation_table_id: PydanticObjectId


@dataclass
class EntityAncestorDescendantMapper:
    """
    EntityAncestorDescendantMapper class is used to construct entity to ancestor/descendant mapping
    """

    entity_id_to_ancestor_ids: Dict[ObjectId, Set[ObjectId]]
    entity_id_to_descendant_ids: Dict[ObjectId, Set[ObjectId]]

    @staticmethod
    def _get_relation_map(
        relationships_info: List[EntityRelationshipInfo], parent_to_child: bool
    ) -> Dict[ObjectId, Set[ObjectId]]:
        """
        Construct relation map

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            Entity relationship info
        parent_to_child: bool
            Whether to construct parent to child relation map or child to parent relation map

        Returns
        -------
        Dict[ObjectId, Set[ObjectId]]
            Relation map
        """
        relation_map: Dict[ObjectId, Set[ObjectId]] = defaultdict(set)
        for relationship_info in relationships_info:
            child_entity_id = relationship_info.entity_id
            parent_entity_id = relationship_info.related_entity_id
            if parent_to_child:
                relation_map[parent_entity_id].add(child_entity_id)
            else:
                relation_map[child_entity_id].add(parent_entity_id)
        return relation_map

    @classmethod
    def _depth_first_search(
        cls,
        ancestors_or_descendants_map: Dict[ObjectId, Set[ObjectId]],
        relation_map: Dict[ObjectId, Set[ObjectId]],
        entity_id: ObjectId,
        ancestor_or_descendant_ids: Set[ObjectId],
        depth: int,
        max_depth: int,
    ) -> None:
        if depth > max_depth:
            return

        ancestors_or_descendants_map[entity_id].update(ancestor_or_descendant_ids)
        for next_entity_id in relation_map[entity_id]:
            cls._depth_first_search(
                ancestors_or_descendants_map=ancestors_or_descendants_map,
                relation_map=relation_map,
                entity_id=next_entity_id,
                ancestor_or_descendant_ids=ancestor_or_descendant_ids | {entity_id},
                depth=depth + 1,
                max_depth=max_depth,
            )

    @classmethod
    def get_entity_id_to_ancestor_ids(
        cls,
        relationships_info: List[EntityRelationshipInfo],
        max_depth: int = ENTITY_RELATIONSHIP_MAX_DEPTH,
    ) -> Dict[ObjectId, Set[ObjectId]]:
        """
        Get entity id to ancestor ids mapping

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            Entity relationship info
        max_depth: int
            Maximum depth to search

        Returns
        -------
        Dict[ObjectId, List[ObjectId]]
            Entity id to ancestor ids mapping
        """
        parent_to_child_entity_ids = cls._get_relation_map(relationships_info, parent_to_child=True)
        entity_id_to_ancestor_ids: Dict[ObjectId, Set[ObjectId]] = defaultdict(set)
        for entity_id in list(parent_to_child_entity_ids):
            cls._depth_first_search(
                ancestors_or_descendants_map=entity_id_to_ancestor_ids,
                relation_map=parent_to_child_entity_ids,
                entity_id=entity_id,
                ancestor_or_descendant_ids=set(),
                depth=0,
                max_depth=max_depth,
            )

        return entity_id_to_ancestor_ids

    @classmethod
    def get_entity_id_to_descendant_ids(
        cls,
        relationships_info: List[EntityRelationshipInfo],
        max_depth: int = ENTITY_RELATIONSHIP_MAX_DEPTH,
    ) -> Dict[ObjectId, Set[ObjectId]]:
        """
        Get entity id to descendant ids mapping

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            Entity relationship info
        max_depth: int
            Maximum depth to search

        Returns
        -------
        Dict[ObjectId, List[ObjectId]]
            Entity id to descendant ids mapping
        """
        child_to_parent_entity_ids = cls._get_relation_map(
            relationships_info, parent_to_child=False
        )
        entity_id_to_descendant_ids: Dict[ObjectId, Set[ObjectId]] = defaultdict(set)
        for entity_id in list(child_to_parent_entity_ids):
            cls._depth_first_search(
                ancestors_or_descendants_map=entity_id_to_descendant_ids,
                relation_map=child_to_parent_entity_ids,
                entity_id=entity_id,
                ancestor_or_descendant_ids=set(),
                depth=0,
                max_depth=max_depth,
            )

        return entity_id_to_descendant_ids

    @classmethod
    def create(
        cls, relationships_info: List[EntityRelationshipInfo]
    ) -> "EntityAncestorDescendantMapper":
        """
        Create a new EntityAncestorDescendantMapper object from the given relationships info

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            Entity relationship info

        Returns
        -------
        EntityAncestorDescendantMapper
        """
        return EntityAncestorDescendantMapper(
            entity_id_to_ancestor_ids=cls.get_entity_id_to_ancestor_ids(relationships_info),
            entity_id_to_descendant_ids=cls.get_entity_id_to_descendant_ids(relationships_info),
        )

    def reduce_entity_ids(self, entity_ids: List[ObjectId]) -> List[ObjectId]:
        """
        Reduce entity IDs to only contain the given entity IDs that are not ancestors of any other entity IDs

        Parameters
        ----------
        entity_ids: List[ObjectId]
            List of entity IDs

        Returns
        -------
        List[ObjectId]
        """
        all_ancestors_ids = set()
        for entity_id in entity_ids:
            all_ancestors_ids.update(self.entity_id_to_ancestor_ids[entity_id])

        reduced_entity_ids = set()
        for entity_id in entity_ids:
            if entity_id not in all_ancestors_ids:
                reduced_entity_ids.add(entity_id)
        return sorted(reduced_entity_ids)
