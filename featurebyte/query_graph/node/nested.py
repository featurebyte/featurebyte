"""
This module contains nested graph related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Sequence, cast

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import GraphNodeType, NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode, NodeT
from featurebyte.query_graph.node.metadata.operation import (
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)


class ProxyInputNode(BaseNode):
    """Proxy input node used by nested graph"""

    class ProxyInputNodeParameters(BaseModel):
        """Proxy input node parameters"""

        input_order: int

    type: Literal[NodeType.PROXY_INPUT] = Field(NodeType.PROXY_INPUT, const=True)
    output_type: NodeOutputType
    parameters: ProxyInputNodeParameters

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # lookup the operation structure using the proxy input's node_name parameter
        proxy_input_order = self.parameters.input_order
        operation_structure = global_state.proxy_input_operation_structures[proxy_input_order]
        return OperationStructure(
            columns=[
                col.clone(node_names=[self.name], node_name=self.name)
                for col in operation_structure.columns
            ],
            aggregations=[
                agg.clone(node_names=[self.name], node_name=self.name)
                for agg in operation_structure.aggregations
            ],
            output_type=operation_structure.output_type,
            output_category=operation_structure.output_category,
            row_index_lineage=(self.name,),
        )


class GraphNodeParameters(BaseModel):
    """Graph node parameters"""

    graph: "QueryGraphModel"  # type: ignore[name-defined]
    output_node_name: str
    type: GraphNodeType


class BaseGraphNode(BaseNode):
    """Graph node"""

    type: Literal[NodeType.GRAPH] = Field(NodeType.GRAPH, const=True)
    output_type: NodeOutputType
    parameters: GraphNodeParameters

    @property
    def transform_info(self) -> str:
        return self.type

    @property
    def output_node(self) -> NodeT:
        """
        Output node of the graph (in the graph node)

        Returns
        -------
        NodeT
        """
        return cast(NodeT, self.parameters.graph.nodes_map[self.parameters.output_node_name])

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # this should not be called as it should be handled at operation structure extractor level
        raise RuntimeError("BaseGroupNode._derive_node_operation_info should not be called!")

    def prune(self: NodeT, target_nodes: Sequence[NodeT]) -> NodeT:
        raise RuntimeError("BaseGroupNode.prune should not be called!")
