"""
This module contains nested graph related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.operation import (
    NodeOutputCategory,
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)


class ProxyInputNode(BaseNode):
    """Proxy input node used by nested graph"""

    class ProxyInputNodeParameters(BaseModel):
        """Proxy input node parameters"""

        node_name: str

    type: Literal[NodeType.PROXY_INPUT] = Field(NodeType.PROXY_INPUT, const=True)
    output_type: NodeOutputType
    parameters: ProxyInputNodeParameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        return OperationStructure(
            output_type=NodeOutputType.FRAME, output_category=NodeOutputCategory.VIEW
        )


class GraphNodeParameters(BaseModel):
    """Graph node parameters"""

    graph: "QueryGraph"  # type: ignore[name-defined]
    output_node_name: str


class BaseGraphNode(BaseNode):
    """Graph node"""

    type: Literal[NodeType.GRAPH] = Field(NodeType.GRAPH, const=True)
    output_type: NodeOutputType
    parameters: GraphNodeParameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        return OperationStructure(
            output_type=NodeOutputType.FRAME, output_category=NodeOutputCategory.VIEW
        )
