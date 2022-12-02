"""
This module contains operation structure extraction related classes.
"""
# pylint: disable=too-few-public-methods
from typing import Any, Dict, List, Set, Tuple

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.transform.base import BaseGraphExtractor


class OperationStructureBranchState(BaseModel):
    """OperationStructureBranchState class"""

    visited_node_types: Set[NodeType] = Field(default_factory=set)


class OperationStructureGlobalState(BaseModel):
    """OperationStructureGlobalState class"""

    operation_structure_map: Dict[str, OperationStructure] = Field(default_factory=dict)


class OperationStructureExtractor(
    BaseGraphExtractor[
        OperationStructure, OperationStructureBranchState, OperationStructureGlobalState
    ],
):
    """OperationStructureExtractorMixin class"""

    def _pre_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureGlobalState,
        node: Node,
        input_node: Node,
    ) -> OperationStructureBranchState:
        return OperationStructureBranchState(
            visited_node_types=branch_state.visited_node_types.union([node.type])
        )

    def _post_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureGlobalState,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        operation_structure = node.derive_node_operation_info(
            inputs=inputs, visited_node_types=branch_state.visited_node_types
        )
        global_state.operation_structure_map[node.name] = operation_structure
        return operation_structure

    def extract(self, node: Node, **kwargs: Any) -> OperationStructure:
        global_state = OperationStructureGlobalState()
        self._extract(
            node=node,
            branch_state=OperationStructureBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state.operation_structure_map[node.name]
