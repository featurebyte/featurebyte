"""
This module contains graph node class.
"""
from typing import Any, Dict, List, Tuple, cast

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.nested import BaseGraphNode, GraphNodeParameters

# update forward references after QueryGraph is defined
GraphNodeParameters.update_forward_refs(QueryGraphModel=QueryGraphModel)


class GraphNode(BaseGraphNode):
    """
    Extend graph node by providing additional graph-node-construction-related methods
    """

    @classmethod
    def create(
        cls,
        node_type: NodeType,
        node_params: Dict[str, Any],
        node_output_type: NodeOutputType,
        input_nodes: List[Node],
    ) -> Tuple["GraphNode", List[Node]]:
        """
        Construct a graph node

        Parameters
        ----------
        node_type: NodeType
            Type of node (to be inserted in the graph inside the graph node)
        node_params: Dict[str, Any]
            Parameters of the node (to be inserted in the graph inside the graph node)
        node_output_type: NodeOutputType
            Output type of the node (to be inserted in the graph inside the graph node)
        input_nodes: List[Nodes]
            Input nodes of the node (to be inserted in the graph inside the graph node)

        Returns
        -------
        Tuple[GraphNode, List[Node]]
        """
        graph = QueryGraphModel()
        proxy_input_nodes = []
        for node in input_nodes:
            proxy_input_node = graph.add_operation(
                node_type=NodeType.PROXY_INPUT,
                node_params={"node_name": node.name},
                node_output_type=node.output_type,
                input_nodes=[],
            )
            proxy_input_nodes.append(proxy_input_node)

        nested_node = graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=node_output_type,
            input_nodes=proxy_input_nodes,
        )
        graph_node = GraphNode(
            name="graph",
            output_type=nested_node.output_type,
            parameters=GraphNodeParameters(graph=graph, output_node_name=nested_node.name),
        )
        return graph_node, proxy_input_nodes

    def add_operation(
        self,
        node_type: NodeType,
        node_params: Dict[str, Any],
        node_output_type: NodeOutputType,
        input_nodes: List[Node],
    ) -> Node:
        """
        Add operation to the query graph inside the graph node

        Parameters
        ----------
        node_type: NodeType
            node type
        node_params: dict
            parameters used for the node operation
        node_output_type: NodeOutputType
            node output type
        input_nodes: list[Node]
            list of input nodes

        Returns
        -------
        Node
            operation node of the given input (from the graph inside the graph node)
        """
        nested_node = self.parameters.graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=node_output_type,
            input_nodes=input_nodes,
        )
        self.parameters.output_node_name = nested_node.name
        return cast(Node, nested_node)
