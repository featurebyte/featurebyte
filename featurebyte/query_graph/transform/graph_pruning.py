"""
This module contains graph pruning related classes.
"""
from typing import Any, Dict, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType
from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import AssignNode, ProjectNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor

NodeNameMap = Dict[str, str]
GraphPruningOutput = Tuple[QueryGraphModel, NodeNameMap]


class GraphPruningBranchState(BaseModel):
    """GraphPruningBranchState class"""


class GraphPruningGlobalState(BaseModel):
    """ "GraphPruningGlobalState class"""

    node_names: Set[str]

    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: NodeNameMap = Field(default_factory=dict)
    processed_node_names: Set[str] = Field(default_factory=set)


class GraphPruningExtractor(
    BaseGraphExtractor[GraphPruningOutput, GraphPruningBranchState, GraphPruningGlobalState]
):
    """GraphPruningExtractor class"""

    def _pre_compute(
        self,
        branch_state: GraphPruningBranchState,
        global_state: GraphPruningGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        # TODO: Decouple AssignNode from pruning logic
        if isinstance(node, AssignNode) and node.name not in global_state.node_names:
            return input_node_names[:1], True
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: GraphPruningBranchState,
        global_state: GraphPruningGlobalState,
        node: Node,
        input_node: Node,
    ) -> GraphPruningBranchState:
        return GraphPruningBranchState()

    def _post_compute(
        self,
        branch_state: GraphPruningBranchState,
        global_state: GraphPruningGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> None:
        if skip_post:
            return

        # construction of the pruned graph
        input_node_names = []
        for input_node_name in self.graph.get_input_node_names(node):
            # if the input node get pruned, it will not exist in the processed_node_names.
            # in this case, keep finding the first parent node exists in the processed_node_names.
            # currently only ASSIGN node could get pruned, the first input node is the frame node.
            # it is used to replace the pruned assigned node
            while input_node_name not in global_state.processed_node_names:
                input_node_name = self.graph.backward_edges_map[input_node_name][0]
            input_node_names.append(input_node_name)

        # construct mapped input_node_names (from original graph to pruned graph)
        mapped_input_nodes = []
        for input_node_name in input_node_names:
            mapped_input_node_name = global_state.node_name_map[input_node_name]
            mapped_input_nodes.append(global_state.graph.get_node_by_name(mapped_input_node_name))

        # add the node back to the prune
        node_pruned = global_state.graph.add_operation(
            node_type=node.type,
            node_params=node.parameters.dict(),
            node_output_type=node.output_type,
            input_nodes=mapped_input_nodes,
        )

        # update the containers to store the mapped node name & processed nodes information
        global_state.node_name_map[node.name] = node_pruned.name
        global_state.processed_node_names.add(node.name)

    def extract(
        self, node: Node, target_columns: Optional[List[str]] = None, **kwargs: Any
    ) -> GraphPruningOutput:
        operation_structure = OperationStructureExtractor(graph=self.graph).extract(node=node)
        if target_columns:
            # subset the operation structure info by keeping only selected columns (using project node)
            temp_node = ProjectNode(
                name="temp",
                parameters={"columns": target_columns},
                output_type=NodeOutputType.FRAME,
            )
            operation_structure = temp_node.derive_node_operation_info(
                inputs=[operation_structure],
                visited_node_types=set(),
            )

        global_state = GraphPruningGlobalState(node_names=operation_structure.all_node_names)
        branch_state = GraphPruningBranchState()
        self._extract(
            node=node,
            branch_state=branch_state,
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state.graph, global_state.node_name_map
