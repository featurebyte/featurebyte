"""
This module contains graph pruning related classes.
"""
from typing import Any, List, Optional, Set, Tuple

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import GraphNodeNameMap, NodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.base import BasePrunableNode
from featurebyte.query_graph.node.generic import ProjectNode
from featurebyte.query_graph.node.metadata.operation import (
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)
from featurebyte.query_graph.node.nested import BaseGraphNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


class NodePruningGlobalState(OperationStructureInfo):
    """NodePruningGlobalState class"""

    # variables for extractor output
    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: NodeNameMap = Field(default_factory=dict)


class NodePruningExtractor(
    OperationStructureExtractor,
    BaseGraphExtractor[GraphNodeNameMap, OperationStructureBranchState, NodePruningGlobalState],
):
    """NodePruningExtractor is used to prune the node parameters (remove redundant parameter values)."""

    def _post_compute(  # type: ignore[override]
        self,
        branch_state: OperationStructureBranchState,
        global_state: NodePruningGlobalState,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        input_op_structs = []
        mapped_input_nodes = []
        for input_node_name in self.graph.get_input_node_names(node):
            input_op_structs.append(global_state.operation_structure_map[input_node_name])
            mapped_input_node_name = global_state.node_name_map[input_node_name]
            mapped_input_nodes.append(global_state.graph.get_node_by_name(mapped_input_node_name))

        if not isinstance(node, BaseGraphNode):
            # prune the parameters if it is not the graph node
            target_node_names = self.graph.edges_map[node.name]
            target_nodes = [
                self.graph.get_node_by_name(node_name) for node_name in target_node_names
            ]
            node = node.prune(
                target_nodes=target_nodes, input_operation_structures=input_op_structs
            )

        node_pruned = global_state.graph.add_operation(
            node_type=node.type,
            node_params=node.parameters.dict(),
            node_output_type=node.output_type,
            input_nodes=mapped_input_nodes,
        )
        global_state.node_name_map[node.name] = node_pruned.name
        return super()._post_compute(
            branch_state=branch_state,
            global_state=global_state,
            node=node_pruned,
            inputs=inputs,
            skip_post=skip_post,
        )

    def extract(  # type: ignore[override]
        self,
        node: Node,
        proxy_input_operation_structures: Optional[List[OperationStructure]] = None,
        target_columns: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> GraphNodeNameMap:
        state_params = {}
        if proxy_input_operation_structures:
            state_params["proxy_input_operation_structures"] = proxy_input_operation_structures

        if target_columns:
            op_struct_info = OperationStructureExtractor(graph=self.graph).extract(
                node=node, proxy_input_operation_structures=proxy_input_operation_structures
            )
            temp_node_name = "temp"

            # subset the operation structure info by keeping only selected columns (using project node)
            temp_node = ProjectNode(
                name=temp_node_name,
                parameters={"columns": target_columns},
                output_type=NodeOutputType.FRAME,
            )
            node = node.prune(
                target_nodes=[temp_node],
                input_operation_structures=[
                    op_struct_info.operation_structure_map[input_node_name]
                    for input_node_name in self.graph.get_input_node_names(node=node)
                ],
            )

        global_state = NodePruningGlobalState(**state_params)
        super()._extract(
            node=node,
            branch_state=OperationStructureBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state.graph, global_state.node_name_map


class GraphPruningBranchState(BaseModel):
    """GraphPruningBranchState class"""


class GraphPruningGlobalState(OperationStructureInfo):
    """GraphPruningGlobalState class"""

    # variables to store some internal pruning info
    node_names: Set[str]  # node names required for the graph output

    # variables for extractor output
    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: NodeNameMap = Field(default_factory=dict)

    # variables to control pruning behavior
    aggressive: bool


class GraphPruningExtractor(
    BaseGraphExtractor[GraphNodeNameMap, GraphPruningBranchState, GraphPruningGlobalState]
):
    """GraphPruningExtractor is used to prune the graph structure (remove redundant nodes)."""

    def _pre_compute(
        self,
        branch_state: GraphPruningBranchState,
        global_state: GraphPruningGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        if isinstance(node, BasePrunableNode) and node.name not in global_state.node_names:
            selected_node_name = node.resolve_node_pruned(input_node_names)
            return [selected_node_name], True
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: GraphPruningBranchState,
        global_state: GraphPruningGlobalState,
        node: Node,
        input_node: Node,
    ) -> GraphPruningBranchState:
        return GraphPruningBranchState()

    @staticmethod
    def _resolve_pruned_node_name(
        graph: QueryGraphModel, node_name_map: NodeNameMap, node_name: str
    ) -> str:
        while node_name not in node_name_map:
            # if the node_name get pruned, it will not exist in the node_name_map
            # in this case, keep finding the replacement node by looking back into the input node names
            node = graph.get_node_by_name(node_name)
            assert isinstance(node, BasePrunableNode)
            input_node_names = graph.get_input_node_names(node)
            node_name = node.resolve_node_pruned(input_node_names)
        return node_name

    @classmethod
    def _prune_nested_graph(
        cls,
        node: BaseGraphNode,
        target_nodes: List[Node],
        proxy_input_operation_structures: List[OperationStructure],
        aggressive: bool,
    ) -> Node:
        nested_graph = node.parameters.graph
        output_node_name = node.parameters.output_node_name
        nested_target_node = nested_graph.get_node_by_name(output_node_name)
        target_columns: Optional[List[str]] = None
        if target_nodes and all(node.type == NodeType.PROJECT for node in target_nodes):
            required_columns = set().union(
                *(node.get_required_input_columns() for node in target_nodes)
            )
            target_columns = list(required_columns)

        pruned_graph, node_name_map = GraphPruningExtractor(graph=nested_graph).extract(
            node=nested_target_node,
            target_columns=target_columns,
            proxy_input_operation_structures=proxy_input_operation_structures,
            aggressive=aggressive,
        )
        output_node_name = cls._resolve_pruned_node_name(
            graph=nested_graph, node_name_map=node_name_map, node_name=output_node_name
        )
        output_node = pruned_graph.get_node_by_name(output_node_name)
        output_graph, pruned_node_name_map = NodePruningExtractor(graph=pruned_graph).extract(
            node=output_node,
            proxy_input_operation_structures=proxy_input_operation_structures,
            target_columns=target_columns,
        )
        return node.clone(
            parameters={
                "graph": output_graph,
                "output_node_name": pruned_node_name_map[output_node_name],
                "type": node.parameters.type,
            }
        )

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
            input_node_name = self._resolve_pruned_node_name(
                graph=self.graph,
                node_name_map=global_state.node_name_map,
                node_name=input_node_name,
            )
            input_node_names.append(input_node_name)

        # construct mapped input_node_names (from original graph to pruned graph)
        mapped_input_nodes = []
        for input_node_name in input_node_names:
            mapped_input_node_name = global_state.node_name_map[input_node_name]
            mapped_input_nodes.append(global_state.graph.get_node_by_name(mapped_input_node_name))

        # add the node back to the pruned graph
        target_node_names = global_state.edges_map[node.name]
        target_nodes = [self.graph.get_node_by_name(node_name) for node_name in target_node_names]
        if global_state.aggressive and isinstance(node, BaseGraphNode):
            proxy_input_operation_structures = [
                global_state.operation_structure_map[node_name]
                for node_name in self.graph.get_input_node_names(node=node)
            ]
            node = self._prune_nested_graph(
                node=node,
                target_nodes=target_nodes,
                proxy_input_operation_structures=proxy_input_operation_structures,
                aggressive=global_state.aggressive,
            )

        node_pruned = global_state.graph.add_operation(
            node_type=node.type,
            node_params=node.parameters.dict(),
            node_output_type=node.output_type,
            input_nodes=mapped_input_nodes,
        )

        # update the containers to store the mapped node name & processed nodes information
        global_state.node_name_map[node.name] = node_pruned.name

    def extract(
        self,
        node: Node,
        target_columns: Optional[List[str]] = None,
        proxy_input_operation_structures: Optional[List[OperationStructure]] = None,
        aggressive: bool = False,
        **kwargs: Any,
    ) -> GraphNodeNameMap:
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(
            node=node, proxy_input_operation_structures=proxy_input_operation_structures
        )
        operation_structure = op_struct_info.operation_structure_map[node.name]
        temp_node_name = "temp"
        if target_columns:
            # subset the operation structure info by keeping only selected columns (using project node)
            temp_node = ProjectNode(
                name=temp_node_name,
                parameters={"columns": target_columns},
                output_type=NodeOutputType.FRAME,
            )
            node = node.prune(
                target_nodes=[temp_node],
                input_operation_structures=[
                    op_struct_info.operation_structure_map[input_node_name]
                    for input_node_name in self.graph.get_input_node_names(node=node)
                ],
            )
            operation_structure = temp_node.derive_node_operation_info(
                inputs=[operation_structure],
                branch_state=OperationStructureBranchState(),
                global_state=OperationStructureInfo(),
            )

        global_state = GraphPruningGlobalState(
            node_names=operation_structure.all_node_names.difference([temp_node_name]),
            edges_map=op_struct_info.edges_map,
            operation_structure_map=op_struct_info.operation_structure_map,
            aggressive=aggressive,
        )
        branch_state = GraphPruningBranchState()
        self._extract(
            node=node,
            branch_state=branch_state,
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state.graph, global_state.node_name_map
