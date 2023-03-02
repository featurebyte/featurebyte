"""
This module contains graph pruning related classes.
"""
from typing import Any, Dict, List, Optional, Set, Tuple

from featurebyte.query_graph.enum import NodeOutputType
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


def map_and_resolve_node_name(
    graph: QueryGraphModel, node_name_map: NodeNameMap, node_name: str
) -> str:
    """
    Map the `node_name` value using `node_name_map`. If the `node_name` is not found in the mapping,
    use its corresponding node to resolve the replacement node.

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph before pruned
    node_name_map: NodeNameMap
        Node name mapping from the original node name to pruned graph node name
    node_name: str
        Node name from the original graph

    Returns
    -------
    str
        Node name in the pruned graph
    """
    while node_name not in node_name_map:
        # if the node_name get pruned, it will not exist in the node_name_map
        # in this case, keep finding the replacement node by looking back into the input node names
        node = graph.get_node_by_name(node_name)
        assert isinstance(node, BasePrunableNode)
        input_node_names = graph.get_input_node_names(node)
        node_name = node.resolve_node_pruned(input_node_names)
    return node_name


def prune_query_graph(
    graph: QueryGraphModel,
    node: Node,
    target_columns: Optional[List[str]] = None,
    proxy_input_operation_structures: Optional[List[OperationStructure]] = None,
    aggressive: bool = False,
) -> Tuple[QueryGraphModel, NodeNameMap, str]:
    """
    Prune the query graph given target node. There are 2 modes in graph pruning:
    - non-aggressive: prune the query graph by doing a graph traversal and keeps all the travelled nodes
    - aggressive: in addition to the graph traversal, further prune the graph by removing the nodes that does not
      contribute to final output and prunes the node parameters by removing unused parameters.

    For aggressive graph pruning, there are 2 major steps:
    - graph structure pruning is performed first by removing useless graph node
    - node parameter pruning is performed then to prune the node parameters on the structure-pruned graph

    Parameters
    ----------
    graph: QueryGraphModel
        Query graph to be pruned
    node: Node
        Target output node
    target_columns: Optional[List[str]]
        Subset of the output columns of the target node, used to further prune the graph. If this parameter
        is provided, the mapped node (of the given node in the pruned graph) could be removed.
    proxy_input_operation_structures: Optional[List[OperationStructure]]
        All ProxyInputNode operation structures for nested graph pruning to map (operation structure of
        ProxyInputNode in the nested graph) to (the operation structure that the proxy input node refers
        to in the external graph)
    aggressive: bool
        Whether to enable aggressive pruning mode. For non-aggressive mode, all travelled nodes will be kept.
        For aggressive mode, node could be removed if it does not contribute to the final output and node
        parameters could be pruned if it is not used.

    Returns
    -------
    Tuple[QueryGraphModel, NodeNameMap, str]
    """
    pruned_graph, node_name_map = GraphStructurePruningExtractor(graph=graph).extract(
        node=node,
        target_columns=target_columns,
        proxy_input_operation_structures=proxy_input_operation_structures,
        aggressive=aggressive,
    )
    if aggressive:
        # if aggressive mode enabled, further prune the node parameters
        # first get the output node name in the pruned graph, use `map_and_resolve_node_name` as the target node
        # could be pruned if `target_columns` is used (means that not all output columns of the target node are
        # required).
        output_node_name = map_and_resolve_node_name(
            graph=graph, node_name_map=node_name_map, node_name=node.name
        )
        mapped_node = pruned_graph.get_node_by_name(node_name_map[output_node_name])
        output_graph, pruned_node_name_map = NodeParametersPruningExtractor(
            graph=pruned_graph
        ).extract(
            node=mapped_node,
            target_columns=target_columns,
            proxy_input_operation_structures=proxy_input_operation_structures,
        )

        # node_name_map => map (original graph node name) to (structure-pruned graph node name)
        # pruned_node_name_map => map (structure-pruned graph node name) to (parameters-pruned graph node name)
        # output_node_name_map => map (original graph node name) to (parameters-pruned graph node name)
        output_node_name_map = {
            key: pruned_node_name_map[value] for key, value in node_name_map.items()
        }
        return output_graph, output_node_name_map, output_node_name_map[output_node_name]
    return pruned_graph, node_name_map, node_name_map[node.name]


class NodeParametersPruningGlobalState(OperationStructureInfo):
    """NodeParametersPruningGlobalState class"""

    def __init__(
        self,
        target_node_name: str,
        graph: Optional[QueryGraphModel] = None,
        node_name_map: Optional[NodeNameMap] = None,
        target_columns: Optional[List[str]] = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.graph = graph or QueryGraphModel()
        self.node_name_map = node_name_map or {}
        self.target_columns = target_columns
        self.target_node_name = target_node_name


class NodeParametersPruningExtractor(
    OperationStructureExtractor,
    BaseGraphExtractor[
        GraphNodeNameMap, OperationStructureBranchState, NodeParametersPruningGlobalState
    ],
):
    """
    NodeParametersPruningExtractor is used to prune the node parameters (remove redundant parameter values).
    This pruning operation will not remove any nodes from the original graph. To prune the node parameters,
    it uses target nodes & input operation structures to prune the node parameters.
    """

    def _post_compute(  # type: ignore[override]
        self,
        branch_state: OperationStructureBranchState,
        global_state: NodeParametersPruningGlobalState,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        if node.name in global_state.node_name_map:
            # if node.name can be found in global_state.node_name_map, it means the node has been inserted
            # into the reconstructed graph.
            pruned_node_name = global_state.node_name_map[node.name]
            return global_state.operation_structure_map[pruned_node_name]

        input_op_structs = []
        mapped_input_nodes = []
        for input_node_name in self.graph.get_input_node_names(node):
            mapped_input_node_name = global_state.node_name_map[input_node_name]
            input_op_structs.append(global_state.operation_structure_map[mapped_input_node_name])
            mapped_input_nodes.append(global_state.graph.get_node_by_name(mapped_input_node_name))

        if not isinstance(node, BaseGraphNode):
            # For the graph node, the pruning happens in GraphStructurePruningExtractor.
            # Prepare target nodes (nodes that consider current node as an input node) & input operation
            # structures to the node. Use these 2 info to perform the actual node parameters pruning.
            target_node_input_order_pairs: List[Tuple[Node, int]] = []
            if node.name == global_state.target_node_name and global_state.target_columns:
                # create a temporary project node if
                # - current node name equals to target_node_name
                # - target_columns is not empty
                project_node = ProjectNode(
                    name="temp",
                    parameters={"columns": global_state.target_columns},
                    output_type=NodeOutputType.FRAME,
                )
                target_node_input_order_pairs.append((project_node, 0))
            else:
                # get the output nodes of current node (target nodes)
                target_node_names = self.graph.edges_map[node.name]
                for target_node_name in target_node_names:
                    target_node = self.graph.get_node_by_name(target_node_name)
                    target_node_input_order_pairs.append(
                        (
                            target_node,
                            self.graph.get_input_node_names(target_node).index(node.name),
                        )
                    )

            node = node.prune(
                target_node_input_order_pairs=target_node_input_order_pairs,
                input_operation_structures=input_op_structs,
            )

        # add the pruned node back to a graph to reconstruct a parameters-pruned graph
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
        state_params: Dict[str, Any] = {"target_node_name": node.name}
        if proxy_input_operation_structures:
            state_params["proxy_input_operation_structures"] = proxy_input_operation_structures

        if target_columns:
            state_params["target_columns"] = target_columns

        global_state = NodeParametersPruningGlobalState(**state_params)
        super()._extract(
            node=node,
            branch_state=OperationStructureBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state.graph, global_state.node_name_map


class GraphPruningBranchState:
    """GraphPruningBranchState class"""


class GraphPruningGlobalState(OperationStructureInfo):
    """GraphPruningGlobalState class"""

    def __init__(
        self,
        node_names: Set[str],
        target_node_name: str,
        graph: Optional[QueryGraphModel] = None,
        node_name_map: Optional[NodeNameMap] = None,
        target_columns: Optional[List[str]] = None,
        aggressive: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        # variables to store some internal pruning info
        self.node_names = node_names

        # variables for extractor output
        self.graph = graph or QueryGraphModel()
        self.node_name_map = node_name_map or {}

        # variables to track output node & target columns
        self.target_columns = target_columns
        self.target_node_name = target_node_name

        # variables to control pruning behavior
        self.aggressive = aggressive


class GraphStructurePruningExtractor(
    BaseGraphExtractor[GraphNodeNameMap, GraphPruningBranchState, GraphPruningGlobalState]
):
    """
    GraphStructurePruningExtractor is used to prune the graph structure (remove redundant nodes).
    This pruning operation travels the graph from the target node back to input nodes (uni-direction).
    For non-aggressive pruning, all the travelled nodes will be kept. For aggressive pruning, the travelled
    nodes will be removed if it does not contribute to the final output.
    """

    def _pre_compute(
        self,
        branch_state: GraphPruningBranchState,
        global_state: GraphPruningGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        if (
            global_state.aggressive
            and isinstance(node, BasePrunableNode)
            and node.name not in global_state.node_names
        ):
            if isinstance(node, BaseGraphNode) and not node.is_prunable:
                # graph node is not prunable
                return input_node_names, False

            # prune the graph structure if
            # - pruning mode is aggressive (means that travelled node can be removed)
            # - node is prunable
            # - node does not contribute to the final output
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

    def _prepare_target_columns(
        self, node: Node, global_state: GraphPruningGlobalState
    ) -> Optional[List[str]]:
        if node.name == global_state.target_node_name:
            # since the target node doesn't have the output node,
            # we use the target columns in the global state
            return global_state.target_columns

        # otherwise, we use the target nodes of current node in the graph to construct the target columns
        target_node_names = global_state.edges_map[node.name]
        target_nodes = [self.graph.get_node_by_name(node_name) for node_name in target_node_names]
        target_columns: Optional[List[str]] = None
        if target_nodes:
            # get the input column order from current node to the target nodes
            target_node_input_order_pairs = []
            for target_node in target_nodes:
                target_node_input_node_names = self.graph.get_input_node_names(node=target_node)
                node_name_input_order = target_node_input_node_names.index(node.name)
                target_node_input_order_pairs.append((target_node, node_name_input_order))

            # construct required column names
            required_columns = set().union(
                *(
                    node.get_required_input_columns(input_index=input_order)
                    for node, input_order in target_node_input_order_pairs
                )
            )
            target_columns = list(required_columns)
        return target_columns

    @classmethod
    def _prune_nested_graph(
        cls,
        node: BaseGraphNode,
        target_columns: Optional[List[str]],
        proxy_input_operation_structures: List[OperationStructure],
        input_nodes: List[Node],
        aggressive: bool,
    ) -> Node:
        nested_graph = node.parameters.graph
        output_node_name = node.parameters.output_node_name
        nested_target_node = nested_graph.get_node_by_name(output_node_name)
        pruned_graph, _, output_node_name = prune_query_graph(
            graph=nested_graph,
            node=nested_target_node,
            target_columns=target_columns,
            proxy_input_operation_structures=proxy_input_operation_structures,
            aggressive=aggressive,
        )
        return node.clone(
            parameters={
                "graph": pruned_graph,
                "output_node_name": output_node_name,
                "type": node.parameters.type,
                "metadata": node.parameters.prune_metadata(
                    target_columns=target_columns,  # type: ignore
                    input_nodes=input_nodes,
                ),
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
            # this implies that the node should be pruned, will not be inserted into the new graph.
            return

        if node.name in global_state.node_name_map:
            # this implies that the node has been inserted into the new graph.
            return

        # construction of the pruned graph
        input_node_names = []
        for input_node_name in self.graph.get_input_node_names(node):
            input_node_name = map_and_resolve_node_name(
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
        if global_state.aggressive and isinstance(node, BaseGraphNode):
            proxy_input_operation_structures = [
                global_state.operation_structure_map[node_name]
                for node_name in self.graph.get_input_node_names(node=node)
            ]
            target_columns = self._prepare_target_columns(node=node, global_state=global_state)
            node = self._prune_nested_graph(
                node=node,
                target_columns=target_columns,
                proxy_input_operation_structures=proxy_input_operation_structures,
                input_nodes=mapped_input_nodes,
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
            node=node,
            proxy_input_operation_structures=proxy_input_operation_structures,
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
            operation_structure = temp_node.derive_node_operation_info(
                inputs=[operation_structure],
                branch_state=OperationStructureBranchState(),
                global_state=OperationStructureInfo(),
            )

        global_state = GraphPruningGlobalState(
            node_names=operation_structure.all_node_names.difference([temp_node_name]),
            edges_map=op_struct_info.edges_map,
            target_node_name=node.name,
            target_columns=target_columns,
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
