"""
RelationshipInfoValidationService class
"""

from dataclasses import dataclass
from itertools import permutations
from typing import Dict, Optional, Tuple

from bson import ObjectId

from featurebyte.exception import InvalidEntityRelationshipError
from featurebyte.models.relationship import RelationshipInfoModel
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
        self.reverse_edges: Dict[ObjectId, list[ObjectId]] = {}
        self.relationship_info_map: Dict[Tuple[ObjectId, ObjectId], ObjectId] = {}

        for relationship_info in all_relationship_info:
            from_entity_id = relationship_info.entity_id
            to_entity_id = relationship_info.related_entity_id

            nodes_set.add(from_entity_id)
            nodes_set.add(to_entity_id)

            if from_entity_id not in self.edges:
                self.edges[from_entity_id] = []
            self.edges[from_entity_id].append(to_entity_id)
            self.reverse_edges.setdefault(to_entity_id, []).append(from_entity_id)

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

    def get_entity_ids_between(
        self, start_entity_id: ObjectId, end_entity_id: ObjectId
    ) -> set[ObjectId]:
        """
        Get the entity IDs that lie on at least one path from start_entity_id to end_entity_id
        (both endpoints included). Empty if end_entity_id is not reachable from start_entity_id.

        Parameters
        ----------
        start_entity_id: ObjectId
            The starting entity ID.
        end_entity_id: ObjectId
            The target entity ID.

        Returns
        -------
        set[ObjectId]
        """

        def _reachable(source: ObjectId, edges: Dict[ObjectId, list[ObjectId]]) -> set[ObjectId]:
            visited = {source}
            stack = [source]
            while stack:
                for next_entity_id in edges.get(stack.pop(), []):
                    if next_entity_id not in visited:
                        visited.add(next_entity_id)
                        stack.append(next_entity_id)
            return visited

        from_start = _reachable(start_entity_id, self.edges)
        if end_entity_id not in from_start:
            return set()
        return from_start & _reachable(end_entity_id, self.reverse_edges)

    def _topological_sort(self, entity_ids: set[ObjectId]) -> Optional[list[ObjectId]]:
        """
        Topologically sort the subgraph induced by entity_ids using Kahn's algorithm.

        Parameters
        ----------
        entity_ids: set[ObjectId]
            Entity IDs to consider.

        Returns
        -------
        Optional[list[ObjectId]]
            Entity IDs in topological order, or None if the subgraph contains a cycle.
        """
        in_degree = {entity_id: 0 for entity_id in entity_ids}
        for entity_id in entity_ids:
            for next_entity_id in self.get_next_entity_ids(entity_id):
                if next_entity_id in in_degree:
                    in_degree[next_entity_id] += 1

        queue = [entity_id for entity_id, degree in in_degree.items() if degree == 0]
        ordered_entity_ids = []
        while queue:
            current_entity_id = queue.pop()
            ordered_entity_ids.append(current_entity_id)
            for next_entity_id in self.get_next_entity_ids(current_entity_id):
                if next_entity_id not in in_degree:
                    continue
                in_degree[next_entity_id] -= 1
                if in_degree[next_entity_id] == 0:
                    queue.append(next_entity_id)

        if len(ordered_entity_ids) != len(entity_ids):
            return None
        return ordered_entity_ids

    def find_longest_path(
        self, start_entity_id: ObjectId, end_entity_id: ObjectId, entity_ids: set[ObjectId]
    ) -> Optional[LookupPath]:
        """
        Find a longest path from start_entity_id to end_entity_id using dynamic programming over
        the subgraph induced by entity_ids. Runs in O(V + E) time, where V is the number of
        entity_ids and E is the number of relationships out of them.

        Parameters
        ----------
        start_entity_id: ObjectId
            The starting entity ID.
        end_entity_id: ObjectId
            The target entity ID.
        entity_ids: set[ObjectId]
            Entity IDs to consider, as returned by get_entity_ids_between.

        Returns
        -------
        Optional[LookupPath]
            A longest path, or None if the subgraph contains a cycle (the DP requires a DAG).
        """
        ordered_entity_ids = self._topological_sort(entity_ids)
        if ordered_entity_ids is None:
            return None

        distance = {start_entity_id: 0}
        parent: Dict[ObjectId, ObjectId] = {}
        for current_entity_id in ordered_entity_ids:
            if current_entity_id not in distance:
                continue
            for next_entity_id in self.get_next_entity_ids(current_entity_id):
                if next_entity_id not in entity_ids:
                    continue
                if distance[current_entity_id] + 1 > distance.get(next_entity_id, -1):
                    distance[next_entity_id] = distance[current_entity_id] + 1
                    parent[next_entity_id] = current_entity_id

        path = [end_entity_id]
        while path[-1] != start_entity_id:
            path.append(parent[path[-1]])
        path.reverse()
        return LookupPath(
            entity_ids=path,
            relationship_info_ids=self.convert_path_to_relationship_info_ids(path),
        )

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
    # Entities on any path from from_entity_id to to_entity_id
    entity_ids_between = relationship_graph.get_entity_ids_between(from_entity_id, to_entity_id)
    if not entity_ids_between:
        return None

    longest_path = relationship_graph.find_longest_path(
        from_entity_id, to_entity_id, entity_ids_between
    )

    # Shortcuts are allowed. Any path is a shortcut if all its entities are included in the longest
    # path, i.e. no entity outside of the longest path can lie on a path between the two entities.
    if longest_path is None or set(longest_path.entity_ids) != entity_ids_between:
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

    The validation checks for each pair of entities whether there is a unique valid path of
    relationships connecting them. When multiple paths exist, only shortcut paths are allowed.

    Cycles in the graph are assumed to have been prevented by validation performed elsewhere.

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
        """
        Validate relationship information between entities.

        Parameters
        ----------
        all_relationship_info : list[RelationshipInfoModel]
            Relationship information to validate.

        Returns
        -------
        ValidatedRelationships
        """
        entity_names_mapping = await self.get_entity_names_mapping(all_relationship_info)
        return validate_relationships(all_relationship_info, entity_names_mapping)

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
