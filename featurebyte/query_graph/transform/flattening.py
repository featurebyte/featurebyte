"""
This module contains graph flattening related classes.
"""
from typing import Dict, List

from pydantic import BaseModel, Field

from featurebyte.query_graph.model.graph import GraphNodeNameMap, QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.nested import BaseGraphNode, ProxyInputNode
from featurebyte.query_graph.transform.base import BaseGraphTransformer


class GraphFlatteningGlobalState(BaseModel):
    """GraphFlatteningGlobalState class"""

    graph: QueryGraphModel = Field(default_factory=QueryGraphModel)
    node_name_map: Dict[str, str] = Field(default_factory=dict)


class GraphFlatteningTransformer(
    BaseGraphTransformer[GraphNodeNameMap, GraphFlatteningGlobalState]
):
    """GraphFlatteningTransformer class"""

    @staticmethod
    def _flatten_nested_graph(
        global_state: GraphFlatteningGlobalState, node: BaseGraphNode, graph_input_nodes: List[Node]
    ) -> None:
        # flatten the nested graph first before inserting those nested graph nodes back to global one
        # nested_flat_node_name_map: nested graph's node name => flattened nested graph's node-name
        nested_flat_graph, nested_flat_node_name_map = GraphFlatteningTransformer(
            graph=node.parameters.graph
        ).transform()

        # nested_node_name_map: flattened nested graph's node name => global_state.graph's node-name
        nested_node_name_map: Dict[str, str] = {}  # nested-node-name => graph-node-name
        for nested_node in nested_flat_graph.iterate_sorted_nodes():
            input_nodes = []
            for nested_input_node_name in nested_flat_graph.get_input_node_names(node=nested_node):
                input_node_name = nested_node_name_map[nested_input_node_name]
                input_nodes.append(global_state.graph.get_node_by_name(input_node_name))

            if isinstance(nested_node, ProxyInputNode):
                input_order = nested_node.parameters.input_order
                nested_node_name_map[nested_node.name] = graph_input_nodes[input_order].name
            else:
                inserted_node = global_state.graph.add_operation(
                    node_type=nested_node.type,
                    node_params=nested_node.parameters.dict(),
                    node_output_type=nested_node.output_type,
                    input_nodes=input_nodes,
                )
                nested_node_name_map[nested_node.name] = inserted_node.name

            # node.parameters.output_node_name refers to the node name in the nested graph
            # we should map it into the flattened nested graph's node name for comparison.
            if nested_node.name == nested_flat_node_name_map[node.parameters.output_node_name]:
                global_state.node_name_map[node.name] = nested_node_name_map[nested_node.name]

    def _compute(self, global_state: GraphFlatteningGlobalState, node: Node) -> None:
        input_nodes = [
            global_state.graph.get_node_by_name(global_state.node_name_map[input_node_name])
            for input_node_name in self.graph.get_input_node_names(node=node)
        ]
        if isinstance(node, BaseGraphNode):
            self._flatten_nested_graph(
                global_state=global_state, node=node, graph_input_nodes=input_nodes
            )
        else:
            inserted_node = global_state.graph.add_operation(
                node_type=node.type,
                node_params=node.parameters.dict(),
                node_output_type=node.output_type,
                input_nodes=input_nodes,
            )
            global_state.node_name_map[node.name] = inserted_node.name

    def transform(self) -> GraphNodeNameMap:
        """
        Transform the graph by flattening all the nested graph.

        Returns
        -------
        GraphNodeNameMap
        """
        global_state = GraphFlatteningGlobalState()
        self._transform(global_state=global_state)
        return global_state.graph, global_state.node_name_map
