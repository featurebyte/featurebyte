"""
RelationshipInfoValidationService class
"""

from dataclasses import dataclass
from itertools import permutations
from typing import Dict, Iterable, Optional, Tuple

from bson import ObjectId

from featurebyte.exception import InvalidEntityRelationshipError
from featurebyte.models.relationship import RelationshipInfoModel, RelationshipType
from featurebyte.service.entity import EntityService


@dataclass
class EntityPairLookupInfo:
    """
    Information about how lookup between two entities is performed.
    """

    from_entity_id: ObjectId
    to_entity_id: ObjectId
    entity_ids: list[ObjectId]
    relationship_info_ids: list[ObjectId]


@dataclass
class ValidatedRelationships:
    """
    Container for validated relationship information.
    """

    all_entity_pair_lookup_info: list[EntityPairLookupInfo]
    unused_relationship_info_ids: list[ObjectId]


@dataclass
class LookupPath:
    """
    Represents a lookup path between two entities.
    """

    entity_ids: list[ObjectId]
    relationship_info_ids: list[ObjectId]


class RelationshipInfoGraph:
    """
    Graph representation of relationship information between entities.
    """

    def __init__(self, all_relationship_info: list[RelationshipInfoModel]):
        nodes_set = set()
        self.edges: Dict[ObjectId, list[ObjectId]] = {}
        self.relationship_info_map: Dict[Tuple[ObjectId, ObjectId], ObjectId] = {}

        for relationship_info in all_relationship_info:
            from_entity_id = relationship_info.entity_id
            to_entity_id = relationship_info.related_entity_id

            nodes_set.add(from_entity_id)
            nodes_set.add(to_entity_id)

            if from_entity_id not in self.edges:
                self.edges[from_entity_id] = []
            self.edges[from_entity_id].append(to_entity_id)

            self.relationship_info_map[(from_entity_id, to_entity_id)] = relationship_info.id

        self.nodes: list[ObjectId] = sorted(nodes_set)

    def get_entity_ids(self) -> list[ObjectId]:
        """
        Get all entity IDs in the graph.

        Returns
        -------
        list[ObjectId]
            List of entity IDs.
        """
        return self.nodes

    def get_next_entity_ids(self, entity_id: ObjectId) -> list[ObjectId]:
        """
        Get the next entity IDs connected to the given entity ID.

        Parameters
        ----------
        entity_id: ObjectId
            The current entity ID.

        Returns
        -------
        list[ObjectId]
            List of connected entity IDs.
        """
        return self.edges.get(entity_id, [])

    def enumerate_paths(
        self, start_entity_id: ObjectId, end_entity_id: ObjectId
    ) -> Iterable[LookupPath]:
        """
        Enumerate all paths from start_entity_id to end_entity_id

        Parameters
        ----------
        start_entity_id: ObjectId
            The starting entity ID.
        end_entity_id: ObjectId
            The target entity ID.

        Yields
        ------
        Iterable[LookupPath]
            Iterable of paths, each represented as a list of relationship info IDs.
        """
        stack = [(start_entity_id, [start_entity_id])]

        while stack:
            current_entity_id, entity_ids = stack.pop()

            if current_entity_id == end_entity_id:
                lookup_path = LookupPath(
                    entity_ids=entity_ids,
                    relationship_info_ids=self.convert_path_to_relationship_info_ids(entity_ids),
                )
                yield lookup_path
                continue

            for next_entity_id in self.get_next_entity_ids(current_entity_id):
                if next_entity_id in entity_ids:
                    continue
                next_entity_ids = entity_ids + [next_entity_id]
                stack.append((next_entity_id, next_entity_ids))

    def convert_path_to_relationship_info_ids(self, path: list[ObjectId]) -> list[ObjectId]:
        """
        Convert a path of entity IDs to a path of relationship info IDs.

        Parameters
        ----------
        path : list[ObjectId]
            List of entity IDs representing the path.

        Returns
        -------
        list[ObjectId]
            List of relationship info IDs corresponding to the path.
        """
        relationship_info_ids = []
        for from_entity_id, to_entity_id in zip(path[:-1], path[1:]):
            relationship_info_id = self.relationship_info_map[(from_entity_id, to_entity_id)]
            relationship_info_ids.append(relationship_info_id)
        return relationship_info_ids


def is_subsequence(needle: list[ObjectId], haystack: list[ObjectId]) -> bool:
    """
    Check if needle is a subsequence of haystack.

    Parameters
    ----------
    needle: list[ObjectId]
        The subsequence to check.
    haystack: list[ObjectId]
        The sequence to check against.

    Returns
    -------
    bool
    """
    pos = 0
    for item in needle:
        # Find the next occurrence of item in haystack[pos:]
        try:
            idx = haystack.index(item, pos)
        except ValueError:
            return False
        pos = idx + 1
    return True


def validate_relationships_single_pair(
    relationship_graph: RelationshipInfoGraph,
    from_entity_id: ObjectId,
    to_entity_id: ObjectId,
    entity_names_mapping: Dict[ObjectId, str],
) -> Optional[EntityPairLookupInfo]:
    """
    Validate relationship information between two entities.

    Parameters
    ----------
    relationship_graph : RelationshipInfoGraph
        Graph representation of relationship information.
    from_entity_id : ObjectId
        The source entity ID.
    to_entity_id : ObjectId
        The target entity ID.
    entity_names_mapping : Dict[ObjectId, str]
        Mapping from entity ID to entity name.

    Returns
    -------
    Optional[EntityPairLookupInfo]
        Entity pair lookup information if valid relationship exists, None otherwise.

    Raises
    ------
    InvalidEntityRelationshipError
        If invalid entity tagging is detected between entities.
    """
    all_paths = list(relationship_graph.enumerate_paths(from_entity_id, to_entity_id))
    if not all_paths:
        return None
    longest_path = max(all_paths, key=lambda x: len(x.relationship_info_ids))
    for path in all_paths:
        if not is_subsequence(path.entity_ids, longest_path.entity_ids):
            from_entity_name = entity_names_mapping.get(from_entity_id)
            to_entity_name = entity_names_mapping.get(to_entity_id)
            raise InvalidEntityRelationshipError(
                f"Invalid entity tagging detected between {from_entity_name} ({from_entity_id})"
                f" and {to_entity_name} ({to_entity_id}). Please review the entities and their"
                " relationships in the catalog."
            )
    entity_pair_lookup_info = EntityPairLookupInfo(
        from_entity_id=from_entity_id,
        to_entity_id=to_entity_id,
        entity_ids=longest_path.entity_ids,
        relationship_info_ids=longest_path.relationship_info_ids,
    )
    return entity_pair_lookup_info


def validate_relationships(
    all_relationship_info: list[RelationshipInfoModel],
    entity_names_mapping: Dict[ObjectId, str],
) -> ValidatedRelationships:
    """
    Validate relationship information between entities.

    Parameters
    ----------
    all_relationship_info : list[RelationshipInfoModel]
        List of all relationship information models.
    entity_names_mapping : Dict[ObjectId, str]
        Mapping from entity ID to entity name.

    Returns
    -------
    ValidatedRelationships
        Container with validated relationship information and unused relationship info IDs.
    """
    relationship_graph = RelationshipInfoGraph(all_relationship_info)

    all_entity_pair_lookup_info: list[EntityPairLookupInfo] = []
    used_relationship_info_ids: set[ObjectId] = set()

    entity_ids = relationship_graph.get_entity_ids()
    for from_entity_id, to_entity_id in permutations(entity_ids, 2):
        entity_pair_lookup_info = validate_relationships_single_pair(
            relationship_graph, from_entity_id, to_entity_id, entity_names_mapping
        )
        if entity_pair_lookup_info is None:
            continue
        for relationship_info_id in entity_pair_lookup_info.relationship_info_ids:
            used_relationship_info_ids.add(relationship_info_id)
        all_entity_pair_lookup_info.append(entity_pair_lookup_info)

    all_relationship_info_ids = {info.id for info in all_relationship_info}
    unused_relationship_info_ids = list(all_relationship_info_ids - used_relationship_info_ids)

    return ValidatedRelationships(
        all_entity_pair_lookup_info=all_entity_pair_lookup_info,
        unused_relationship_info_ids=unused_relationship_info_ids,
    )


class RelationshipInfoValidationService:
    """
    Service for validating relationship information between entities.
    """

    def __init__(self, entity_service: EntityService):
        self.entity_service = entity_service

    async def validate_relationships(
        self,
        all_relationship_info: list[RelationshipInfoModel],
    ) -> ValidatedRelationships:
        child_parent_infos = [
            info
            for info in all_relationship_info
            if info.relationship_type == RelationshipType.CHILD_PARENT
        ]
        entity_names_mapping = await self.get_entity_names_mapping(child_parent_infos)
        return validate_relationships(child_parent_infos, entity_names_mapping)

    async def get_entity_names_mapping(
        self, relationship_infos: list[RelationshipInfoModel]
    ) -> Dict[ObjectId, str]:
        """
        Get mapping from entity ID to entity name.

        Parameters
        ----------
        relationship_infos: list[RelationshipInfoModel]
            List of relationship information.

        Returns
        -------
        Dict[ObjectId, str]
            Mapping from entity ID to entity name.
        """
        entity_ids = set()
        for info in relationship_infos:
            entity_ids.add(info.entity_id)
            entity_ids.add(info.related_entity_id)
        mapping = {}
        async for doc in self.entity_service.list_documents_as_dict_iterator(
            query_filter={"_id": {"$in": list(entity_ids)}}
        ):
            mapping[doc["_id"]] = doc["name"]
        return mapping
