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
from featurebyte.query_graph.node.nested import BaseGraphNode
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
        global_state.edges_map[input_node.name].add(node.name)
        return OperationStructureBranchState(
            visited_node_types=branch_state.visited_node_types.union([node.type])
        )

    @staticmethod
    def _prepare_operation_structure(
        node: BaseGraphNode,
        operation_structure: OperationStructure,
        operation_structure_map: Dict[str, OperationStructure],
    ) -> OperationStructure:
        # find the proxy input nodes from the nested graph to create proxy node name to outer node names mapping
        # (to remap the proxy node name in the nested graph back to outer graph)
        nested_graph = node.parameters.graph
        nested_target_node = nested_graph.get_node_by_name(node.parameters.output_node_name)
        proxy_input_node_name_map = {}
        for proxy_input_node in nested_graph.iterate_nodes(
            target_node=nested_target_node, node_type=NodeType.PROXY_INPUT
        ):
            ref_node_name = proxy_input_node.parameters.node_name
            proxy_input_node_name_map[proxy_input_node.name] = operation_structure_map[
                ref_node_name
            ].all_node_names

        # update node_names of the nested operation structure so that the internal node names (node names only
        # appears in the nested graph are removed)
        clone_kwargs = {
            "proxy_node_name_map": proxy_input_node_name_map,
            "graph_node_name": node.name,
            "graph_node_transform": node.transform_info,
        }
        return OperationStructure(
            columns=[
                col.clone_without_internal_nodes(**clone_kwargs)  # type: ignore
                for col in operation_structure.columns
            ],
            aggregations=[
                agg.clone_without_internal_nodes(**clone_kwargs)  # type: ignore
                for agg in operation_structure.aggregations
            ],
            output_type=operation_structure.output_type,
            output_category=operation_structure.output_category,
            is_time_based=operation_structure.is_time_based,
        )

    def _derive_nested_graph_operation_structure(
        self, node: BaseGraphNode, input_operation_structures: List[OperationStructure]
    ) -> OperationStructure:
        # extract operation_structure of the nested graph
        node_params = node.parameters
        nested_graph = node_params.graph
        nested_output_node_name = node_params.output_node_name
        nested_output_node = nested_graph.get_node_by_name(nested_output_node_name)
        # operation structure map contains inputs to the graph node
        # so that proxy input node can refer to them
        input_node_names = self.graph.get_input_node_names(node=node)
        operation_structure_map = dict(zip(input_node_names, input_operation_structures))
        nested_op_structure_info = OperationStructureExtractor(graph=nested_graph).extract(
            node=nested_output_node,
            operation_structure_map=operation_structure_map,
        )
        nested_operation_structure = nested_op_structure_info.operation_structure_map[
            nested_output_node_name
        ]
        return self._prepare_operation_structure(
            node=node,
            operation_structure=nested_operation_structure,
            operation_structure_map=operation_structure_map,
        )

    def _post_compute(
        self,
        branch_state: OperationStructureBranchState,
        global_state: OperationStructureInfo,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        if isinstance(node, BaseGraphNode):
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
