"""
Parent / child entity lookup related models
"""
from __future__ import annotations

from typing import Dict, Iterator, List, Optional, Sequence, Set, Tuple

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte.exception import RequiredEntityNotProvidedError
from featurebyte.models.base import PydanticObjectId
from featurebyte.query_graph.model.entity_relationship_info import EntityRelationshipInfo


def sorted_entity_ids(entity_ids: Sequence[ObjectId]) -> Tuple[ObjectId, ...]:
    """
    Return a sorted tuple of entity ids

    Parameters
    ----------
    entity_ids: Union[Tuple[PydanticObjectId, ...], List[PydanticObjectId]]]
        List or tuple of entity ids

    Returns
    -------
    Tuple[PydanticObjectId, ...]
    """
    return tuple(sorted([ObjectId(entity_id) for entity_id in entity_ids]))


@dataclass
class EntityLookupPlan:
    """
    EntityLookupPlan contains the lookup steps for a specific feature table

    """

    feature_primary_entity_ids: Sequence[ObjectId]
    descendant_ids: Set[ObjectId]
    lookup_steps_mapping: Dict[Tuple[ObjectId, ...], List[EntityRelationshipInfo]]

    def get_entity_lookup_steps(
        self, serving_entity_ids: Sequence[ObjectId]
    ) -> Optional[List[EntityRelationshipInfo]]:
        """
        Get the parent entity lookup steps required to convert serving entity ids to feature table's
        primary entity ids

        Parameters
        ----------
        serving_entity_ids: List[PydanticObjectId]
            Serving entity ids to query

        Returns
        -------
        Optional[List[EntityRelationshipInfo]]
        """
        key = sorted_entity_ids(
            [entity_id for entity_id in serving_entity_ids if entity_id in self.descendant_ids]
        )
        return self.lookup_steps_mapping.get(key)


@dataclass
class EntityLookupState:
    """
    A step in the entity lookup BFS traversal
    """

    entity_ids: Sequence[ObjectId]
    lookup_path: List[EntityRelationshipInfo]

    def apply_relationship(self, relationship: EntityRelationshipInfo) -> EntityLookupState:
        """
        Return a new EntityLookupState by applying a relationship that lead to its parent or child

        Parameters
        ----------
        relationship: EntityRelationshipInfo
            EntityRelationshipInfo object representing relationship between two entities

        Returns
        -------
        EntityLookupState
        """
        new_entity_ids = []
        for entity_id in self.entity_ids:
            if entity_id == relationship.related_entity_id:
                new_entity_ids.append(relationship.entity_id)  # child
            elif entity_id == relationship.entity_id:
                new_entity_ids.append(relationship.related_entity_id)  # parent
        return EntityLookupState(
            entity_ids=new_entity_ids,
            lookup_path=[relationship] + self.lookup_path,
        )


class EntityLookupPlanner:
    """
    EntityLookupPlanner is responsible for determining how entity lookup should be performed
    """

    @classmethod
    def generate_plan(
        cls,
        feature_primary_entity_ids: List[PydanticObjectId],
        relationships_info: List[EntityRelationshipInfo],
    ) -> EntityLookupPlan:
        """
        Generate an EntityLookupPlan object with all the join steps pre-calculated for all posssible
        candidate serving entities

        Parameters
        ----------
        feature_primary_entity_ids: List[PydanticObjectId]
            Primary entity ids of the feature / feature table
        relationships_info: List[EntityRelationshipInfo]
            Relationships available for use as recorded in the FeatureList

        Returns
        -------
        EntityLookupPlan
        """
        relationships_mapping = cls._get_relationships_to_children(relationships_info)
        pending = [
            EntityLookupState(
                entity_ids=list(feature_primary_entity_ids),
                lookup_path=[],
            )
        ]
        lookup_steps_mapping = {}
        descendant_ids = set()
        for node in cls._bfs(
            relationships_mapping=relationships_mapping,
            pending=pending,
        ):
            key = sorted_entity_ids(node.entity_ids)
            if key not in lookup_steps_mapping:
                lookup_steps_mapping[key] = node.lookup_path
                for new_entity_id in node.entity_ids:
                    if new_entity_id not in feature_primary_entity_ids:
                        descendant_ids.add(new_entity_id)

        return EntityLookupPlan(
            feature_primary_entity_ids=feature_primary_entity_ids,
            lookup_steps_mapping=lookup_steps_mapping,
            descendant_ids=descendant_ids,
        )

    @classmethod
    def generate_lookup_steps(
        cls,
        available_entity_ids: Sequence[ObjectId],
        required_entity_ids: Sequence[ObjectId],
        relationships_info: List[EntityRelationshipInfo],
    ) -> List[EntityRelationshipInfo]:
        """
        Generate a list of required lookup steps to retrieve missing parent entities based on the
        available relationships.

        Used by online serving service to determine the required parent entity lookups.

        Parameters
        ----------
        available_entity_ids: List[PydanticObjectId]
            Available entity ids provided in online serving request
        required_entity_ids: List[PydanticObjectId]
            Required entity ids for serving
        relationships_info: List[EntityRelationshipInfo]
            Relationships available for use as recorded in the FeatureList

        Returns
        -------
        List[EntityRelationshipInfo]

        Raises
        ------
        RequiredEntityNotProvidedError
            When one or more required entities are not provided and cannot be automatically
            retrieved via available relationships
        """
        missing_entity_ids = set(required_entity_ids).difference(available_entity_ids)
        relationships_mapping = cls._get_relationships_to_parents(relationships_info)
        pending = [
            EntityLookupState(
                entity_ids=list(available_entity_ids),
                lookup_path=[],
            )
        ]
        required_lookup_steps = []
        for node in cls._bfs(
            relationships_mapping=relationships_mapping,
            pending=pending,
        ):
            required = False
            for entity_id in node.entity_ids:
                if entity_id in missing_entity_ids:
                    missing_entity_ids.remove(entity_id)
                    required = True
                    break
            if required:
                for lookup_step in node.lookup_path[::-1]:
                    if lookup_step not in required_lookup_steps:
                        required_lookup_steps.append(lookup_step)
            if not missing_entity_ids:
                break

        if len(missing_entity_ids) > 0:
            raise RequiredEntityNotProvidedError(missing_entity_ids=missing_entity_ids)
        assert len(missing_entity_ids) == 0, "Missing entities cannot be fulfilled"
        return required_lookup_steps

    @classmethod
    def _get_relationships_to_children(
        cls,
        relationships_info: List[EntityRelationshipInfo],
    ) -> Dict[ObjectId, List[EntityRelationshipInfo]]:
        """
        Construct a mapping from entity id to a list of relationships that lead to its children

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            Available relationships

        Returns
        -------
        Dict[PydanticObjectId, List[EntityRelationshipInfo]]
        """
        graph = defaultdict(list)
        for relationship in relationships_info:
            parent_id = ObjectId(relationship.related_entity_id)
            graph[parent_id].append(relationship)
        return dict(graph)

    @classmethod
    def _get_relationships_to_parents(
        cls,
        relationships_info: List[EntityRelationshipInfo],
    ) -> Dict[ObjectId, List[EntityRelationshipInfo]]:
        """
        Construct a mapping from entity id to a list of relationships that lead to its parents

        Parameters
        ----------
        relationships_info: List[EntityRelationshipInfo]
            Available relationships

        Returns
        -------
        Dict[PydanticObjectId, List[EntityRelationshipInfo]]
        """
        graph = defaultdict(list)
        for relationship in relationships_info:
            child_id = ObjectId(relationship.entity_id)
            graph[child_id].append(relationship)
        return dict(graph)

    @classmethod
    def _bfs(
        cls,
        relationships_mapping: Dict[ObjectId, List[EntityRelationshipInfo]],
        pending: List[EntityLookupState],
    ) -> Iterator[EntityLookupState]:
        visited = set()
        while pending:
            current_node, pending = pending[0], pending[1:]
            yield current_node
            for entity_id in current_node.entity_ids:
                relationships = relationships_mapping.get(entity_id, None)
                if not relationships:
                    continue
                for relationship in relationships:
                    new_node = current_node.apply_relationship(relationship)
                    key = sorted_entity_ids(new_node.entity_ids)
                    if key not in visited:
                        visited.add(key)
                        pending.append(new_node)
