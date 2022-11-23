"""
This module contains nested graph related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Set

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure


class NestedGraphMixin:
    """Mixin shared by nested graph related node"""

    # pylint: disable=too-few-public-methods

    def derive_node_operation_info(
        self, inputs: List[OperationStructure], visited_node_types: Set[NodeType]
    ) -> OperationStructure:
        """
        Derive node operation info

        Parameters
        ----------
        inputs: List[OperationStructure]
            List of input nodes' operation info
        visited_node_types: Set[NodeType]
            Set of visited nodes when doing backward traversal

        Returns
        -------
        OperationStructure
        """
        # TODO: implement this method
        _ = inputs, visited_node_types
        return OperationStructure()


class ProxyInputNode(NestedGraphMixin, BaseNode):
    """Proxy input node used by nested graph"""

    class ProxyInputNodeParameters(BaseModel):
        """Proxy input node parameters"""

        node_name: str

    type: Literal[NodeType.PROXY_INPUT] = Field(NodeType.PROXY_INPUT, const=True)
    output_type: NodeOutputType
    parameters: ProxyInputNodeParameters


class GraphNodeParameters(BaseModel):
    """Graph node parameters"""

    graph: "QueryGraph"  # type: ignore[name-defined]
    output_node_name: str


class BaseGraphNode(NestedGraphMixin, BaseNode):
    """Graph node"""

    type: Literal[NodeType.GRAPH] = Field(NodeType.GRAPH, const=True)
    output_type: NodeOutputType
    parameters: GraphNodeParameters
