"""
This module contains operation structure extraction related classes.
"""
from typing import Any, List, Tuple

from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import (
    OperationStructure,
    OperationStructureBranchState,
    OperationStructureInfo,
)
from featurebyte.query_graph.transform.base import BaseGraphExtractor


class OperationStructureExtractor(
    BaseGraphExtractor[
        OperationStructureInfo, OperationStructureBranchState, OperationStructureInfo
    ],
):
    """OperationStructureExtractor class"""

    def _pre_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
        node: Node,
        input_node: Node,
    ) -> OperationStructureBranchState:
        return OperationStructureBranchState(
            visited_node_types=branch_state.visited_node_types.union([node.type])
        )

    def _post_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        operation_structure = node.derive_node_operation_info(
            inputs=inputs,
            branch_state=branch_state,
            global_state=global_state,
        )
        global_state.operation_structure_map[node.name] = operation_structure
        return operation_structure

    def extract(self, node: Node, **kwargs: Any) -> OperationStructureInfo:
        global_state = OperationStructureInfo()
        self._extract(
            node=node,
            branch_state=OperationStructureBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
