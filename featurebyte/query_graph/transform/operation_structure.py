"""
This module contains operation structure extraction related classes.
"""
from typing import Any, Dict, List, Optional, Tuple

from featurebyte.query_graph.enum import NodeType
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

    def _derive_nested_graph_operation_structure(
        self, node: Node, input_operation_structures: List[OperationStructure]
    ) -> OperationStructure:
        node_params = node.parameters
        nested_graph = node_params.graph  # type: ignore
        nested_output_node_name = node_params.output_node_name  # type: ignore
        nested_output_node = nested_graph.get_node_by_name(nested_output_node_name)
        input_node_names = self.graph.get_input_node_names(node=node)
        operation_structure_map = dict(zip(input_node_names, input_operation_structures))
        op_structure_info = OperationStructureExtractor(graph=nested_graph).extract(
            node=nested_output_node,
            operation_structure_map=operation_structure_map,
        )
        return op_structure_info.operation_structure_map[nested_output_node_name]

    def _post_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        if node.type == NodeType.GRAPH:
            operation_structure = self._derive_nested_graph_operation_structure(
                node=node,
                input_operation_structures=inputs,
            )
        else:
            operation_structure = node.derive_node_operation_info(
                inputs=inputs,
                branch_state=branch_state,
                global_state=global_state,
            )

        global_state.operation_structure_map[node.name] = operation_structure
        return operation_structure

    def extract(
        self,
        node: Node,
        operation_structure_map: Optional[Dict[str, OperationStructure]] = None,
        **kwargs: Any,
    ) -> OperationStructureInfo:
        state_params = {}
        if operation_structure_map:
            state_params["operation_structure_map"] = operation_structure_map

        global_state = OperationStructureInfo(**state_params)
        self._extract(
            node=node,
            branch_state=OperationStructureBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
