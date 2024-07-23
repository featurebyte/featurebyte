"""
This module contains graph node class.
"""

from typing import Any, Dict, List, Optional, Tuple, cast

from featurebyte.common.model_util import construct_serialize_function
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.nested import (
    GRAPH_NODE_PARAMETERS_TYPES,
    BaseGraphNode,
    GraphNodeParameters,
)

# update forward references after QueryGraph is defined
for graph_node_parameters_type in GRAPH_NODE_PARAMETERS_TYPES:
    graph_node_parameters_type.model_rebuild()


# construct function for graph node parameter deserialization
construct_graph_node_parameter = construct_serialize_function(
    all_types=GRAPH_NODE_PARAMETERS_TYPES,
    annotated_type=GraphNodeParameters,
    discriminator_key="type",
)


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
        graph_node_type: GraphNodeType,
        nested_node_input_indices: Optional[List[int]] = None,
        metadata: Optional[FeatureByteBaseModel] = None,
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
        graph_node_type: GraphNodeType
            Type of graph node
        nested_node_input_indices: Optional[List[int]]
            Indices of input nodes to be used as input nodes of the nested node
        metadata: Optional[FetaureByteBaseModel]
            Optional metadata that is passed to the graph node parameters

        Returns
        -------
        Tuple[GraphNode, List[Node]]
        """
        graph = QueryGraphModel()
        proxy_input_nodes = []
        for i, node in enumerate(input_nodes):
            proxy_input_node = graph.add_operation(
                node_type=NodeType.PROXY_INPUT,
                node_params={"input_order": i},
                node_output_type=node.output_type,
                input_nodes=[],
            )
            proxy_input_nodes.append(proxy_input_node)

        # prepare input nodes for the nested node (if not all nodes in the input_nodes are to be used)
        nested_node_inputs = proxy_input_nodes
        if nested_node_input_indices:
            nested_node_inputs = [proxy_input_nodes[idx] for idx in nested_node_input_indices]

        nested_node = graph.add_operation(
            node_type=node_type,
            node_params=node_params,
            node_output_type=node_output_type,
            input_nodes=nested_node_inputs,
        )

        graph_node_parameters = {
            "graph": graph,
            "output_node_name": nested_node.name,
            "type": graph_node_type,
        }
        if metadata:
            graph_node_parameters["metadata"] = metadata

        graph_node = GraphNode(
            name="graph",
            output_type=nested_node.output_type,
            parameters=construct_graph_node_parameter(**graph_node_parameters),
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
