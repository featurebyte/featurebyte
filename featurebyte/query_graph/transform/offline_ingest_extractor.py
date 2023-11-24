"""
This module contains offline store ingest query extraction related classes.
"""
from typing import Any, Dict, List, Optional, Tuple

from collections import defaultdict
from dataclasses import dataclass

from bson import ObjectId

from featurebyte.query_graph.graph_node.base import GraphNode
from featurebyte.query_graph.model.entity_relationship_info import (
    EntityAncestorDescendantMapper,
    EntityRelationshipInfo,
)
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import LookupNode, LookupTargetNode
from featurebyte.query_graph.node.mixin import BaseGroupbyParameters
from featurebyte.query_graph.node.nested import OfflineStoreIngestQueryGraphNodeParameters
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.quick_pruning import QuickGraphStructurePruningTransformer


@dataclass
class OfflineStoreIngestQueryGlobalState:
    """OfflineStoreIngestQueryGlobalState class"""

    # decomposed graph
    graph: QueryGraphModel
    # original graph node name to decomposed graph node name mapping
    node_name_map: Dict[str, str]
    # entity id to ancestor/descendant mapping
    entity_ancestor_descendant_mapper: EntityAncestorDescendantMapper
    # (original graph) node name to primary entity ids mapping
    node_name_to_primary_entity_ids: Dict[str, List[ObjectId]]
    # whether the graph is decomposed or not
    is_decomposed: bool = False

    @classmethod
    def create(
        cls, relationships_info: Optional[List[EntityRelationshipInfo]]
    ) -> "OfflineStoreIngestQueryGlobalState":
        """
        Create a new OfflineStoreIngestQueryGlobalState object from the given relationships info

        Parameters
        ----------
        relationships_info: Optional[List[EntityRelationshipInfo]]
            Entity relationship info

        Returns
        -------
        OfflineStoreIngestQueryGlobalState
        """
        return OfflineStoreIngestQueryGlobalState(
            graph=QueryGraphModel(),
            entity_ancestor_descendant_mapper=EntityAncestorDescendantMapper.create(
                relationships_info=relationships_info or []
            ),
            node_name_to_primary_entity_ids=defaultdict(list),
            node_name_map={},
        )

    def update_primary_entity_ids(self, node: Node, input_node_names: List[str]) -> None:
        """
        Update primary entity IDs for the given node name

        Parameters
        ----------
        node: Node
            Node to be processed
        input_node_names: List[str]
            List of input node names
        """
        primary_entity_ids: List[ObjectId]
        if isinstance(node.parameters, BaseGroupbyParameters):
            # primary entity ids introduced by groupby node family
            primary_entity_ids = node.parameters.entity_ids or []  # type: ignore
        elif isinstance(node, (LookupNode, LookupTargetNode)):
            # primary entity ids introduced by lookup node family
            primary_entity_ids = [node.parameters.entity_id]
        else:
            # primary entity ids inherited from input nodes
            primary_entity_ids = []
            for input_node_name in input_node_names:
                primary_entity_ids.extend(self.node_name_to_primary_entity_ids[input_node_name])
            primary_entity_ids = list(set(primary_entity_ids))

        # reduce the primary entity ids based on entity relationship
        primary_entity_ids = self.entity_ancestor_descendant_mapper.reduce_entity_ids(
            entity_ids=list(primary_entity_ids)
        )
        self.node_name_to_primary_entity_ids[node.name] = sorted(primary_entity_ids)

    def should_decompose_query_graph(self, node_name: str, input_node_names: List[str]) -> bool:
        """
        Check whether to decompose the query graph into graph with nested offline store ingest query nodes

        Parameters
        ----------
        node_name: str
            Node name
        input_node_names: List[str]
            List of input node names

        Returns
        -------
        bool
        """
        if not input_node_names:
            # if there is no input node, that means this is the starting node
            return False

        output_entity_ids = self.node_name_to_primary_entity_ids[node_name]
        all_input_entity_are_empty = True
        for input_node_name in input_node_names:
            input_entity_ids = self.node_name_to_primary_entity_ids[input_node_name]
            if input_entity_ids == output_entity_ids:
                # if any of the input is the same as the output, that means no new entity ids are added
                # to the universe. So we should not split the query graph.
                return False
            if input_entity_ids:
                all_input_entity_are_empty = False

        if all_input_entity_are_empty:
            # if all the input nodes are empty, that means the output node is the starting node
            # that introduces new entity ids to the universe. So we should not split the query graph.
            return False

        # if none of the above conditions are met, that means we should split the query graph
        return True

    def add_operation_to_graph(self, node: Node, input_nodes: List[Node]) -> Node:
        """
        Add operation to the graph

        Parameters
        ----------
        node: Node
            Node to be added
        input_nodes: List[Node]
            List of input nodes

        Returns
        -------
        Node
            Added node
        """
        inserted_node = self.graph.add_operation(
            node_type=node.type,
            node_params=node.parameters.dict(by_alias=True),
            node_output_type=node.output_type,
            input_nodes=input_nodes,
        )
        self.node_name_map[node.name] = inserted_node.name
        return inserted_node

    def get_mapped_decomposed_graph_node(self, node_name: str) -> Node:
        """
        Get the mapped decomposed graph node for the given node name

        Parameters
        ----------
        node_name: str
            Node name (of the original graph)

        Returns
        -------
        Node
            Decomposed graph node
        """
        return self.graph.get_node_by_name(self.node_name_map[node_name])


@dataclass
class OfflineStoreIngestQueryBranchState:
    """OfflineStoreIngestQueryBranchState class"""


class OfflineStoreIngestQueryExtractor(
    BaseGraphExtractor[
        OfflineStoreIngestQueryGlobalState,
        OfflineStoreIngestQueryBranchState,
        OfflineStoreIngestQueryGlobalState,
    ]
):
    """
    OfflineStoreIngestExtractor class

    This class is used to decompose a query graph into a query graph with
    - offline store ingest query nested graph nodes
    - post offline store ingest processing nodes
    """

    def _pre_compute(
        self,
        branch_state: OfflineStoreIngestQueryBranchState,
        global_state: OfflineStoreIngestQueryGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: OfflineStoreIngestQueryBranchState,
        global_state: OfflineStoreIngestQueryGlobalState,
        node: Node,
        input_node: Node,
    ) -> OfflineStoreIngestQueryBranchState:
        return branch_state

    def _insert_offline_store_ingest_query_node(
        self, global_state: OfflineStoreIngestQueryGlobalState, node_name: str
    ) -> Node:
        """
        Insert offline store ingest query node to the decomposed graph

        Parameters
        ----------
        global_state: OfflineStoreIngestQueryGlobalState
            OfflineStoreIngestQueryGlobalState object
        node_name: str
            Node name of the original graph that is used to create the offline store ingest query node

        Returns
        -------
        Node
            Added node (of the decomposed graph)
        """
        transformer = QuickGraphStructurePruningTransformer(graph=self.graph)
        subgraph, node_name_to_transformed_node_name = transformer.transform(
            target_node_names=[node_name]
        )
        transformed_node = subgraph.get_node_by_name(node_name_to_transformed_node_name[node_name])
        graph_node = GraphNode(
            name="graph",
            output_type=transformed_node.output_type,
            parameters=OfflineStoreIngestQueryGraphNodeParameters(
                graph=subgraph,
                output_node_name=transformed_node.name,
            ),
        )
        return global_state.add_operation_to_graph(node=graph_node, input_nodes=[])

    def _post_compute(
        self,
        branch_state: OfflineStoreIngestQueryBranchState,
        global_state: OfflineStoreIngestQueryGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> None:
        input_node_names = self.graph.get_input_node_names(node)
        global_state.update_primary_entity_ids(node=node, input_node_names=input_node_names)

        if not global_state.is_decomposed:
            # check whether to decompose the query graph
            is_decomposed = global_state.should_decompose_query_graph(
                node_name=node.name, input_node_names=input_node_names
            )
            if is_decomposed:
                # insert offline store ingest query node
                decom_input_nodes = []
                for input_node_name in input_node_names:
                    added_node = self._insert_offline_store_ingest_query_node(
                        global_state=global_state, node_name=input_node_name
                    )
                    decom_input_nodes.append(added_node)

                # add current node to the decomposed graph
                global_state.add_operation_to_graph(node=node, input_nodes=decom_input_nodes)

            # update global state
            global_state.is_decomposed = is_decomposed
        else:
            # if the graph is already decided to be decomposed
            # first, check if any of the input node has its corresponding mapping in the decomposed graph
            has_input_associated_with_decomposed_graph = any(
                input_node_name in global_state.node_name_map
                for input_node_name in input_node_names
            )
            if has_input_associated_with_decomposed_graph:
                # if any of the input node has its corresponding mapping in the decomposed graph
                # that means we should insert the current node to the decomposed graph.
                decom_input_nodes = []
                for input_node_name in input_node_names:
                    if input_node_name in global_state.node_name_map:
                        decom_input_nodes.append(
                            global_state.get_mapped_decomposed_graph_node(node_name=input_node_name)
                        )
                    else:
                        decom_input_nodes.append(
                            self._insert_offline_store_ingest_query_node(
                                global_state=global_state, node_name=input_node_name
                            )
                        )

                # add current node to the decomposed graph
                global_state.add_operation_to_graph(node=node, input_nodes=decom_input_nodes)

    def extract(
        self,
        node: Node,
        relationships_info: Optional[List[EntityRelationshipInfo]] = None,
        **kwargs: Any,
    ) -> OfflineStoreIngestQueryGlobalState:
        global_state = OfflineStoreIngestQueryGlobalState.create(
            relationships_info=relationships_info
        )
        branch_state = OfflineStoreIngestQueryBranchState()
        self._extract(
            node=node,
            branch_state=branch_state,
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state