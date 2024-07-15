"""
This module contains operation structure extraction related classes.
"""

from typing import Any, Dict, List, Optional, Tuple

from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import (
    OperationStructure,
    OperationStructureInfo,
)
from featurebyte.query_graph.node.nested import BaseGraphNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor


class OperationStructureExtractor(
    BaseGraphExtractor[OperationStructureInfo, FeatureByteBaseModel, OperationStructureInfo],
):
    """OperationStructureExtractor class"""

    def _pre_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OperationStructureInfo,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OperationStructureInfo,
        node: Node,
        input_node: Node,
    ) -> FeatureByteBaseModel:
        global_state.edges_map[input_node.name].add(node.name)
        return branch_state

    @staticmethod
    def _prepare_operation_structure(
        node: BaseGraphNode,
        operation_structure: OperationStructure,
        proxy_input_operation_structures: List[OperationStructure],
    ) -> OperationStructure:
        # find the proxy input nodes from the nested graph to create proxy node name to outer node names mapping
        # (to remap the proxy node name in the nested graph back to outer graph)
        nested_graph = node.parameters.graph
        nested_target_node = nested_graph.get_node_by_name(node.parameters.output_node_name)
        proxy_input_node_name_map = {}
        for proxy_input_node in nested_graph.iterate_nodes(
            target_node=nested_target_node, node_type=NodeType.PROXY_INPUT
        ):
            input_order = proxy_input_node.parameters.input_order
            proxy_input_node_name_map[proxy_input_node.name] = proxy_input_operation_structures[
                input_order
            ]

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
            row_index_lineage=operation_structure.row_index_lineage,
            is_time_based=operation_structure.is_time_based,
        )

    def _derive_nested_graph_operation_structure(
        self,
        node: BaseGraphNode,
        input_operation_structures: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        # extract operation_structure of the nested graph
        node_params = node.parameters
        nested_graph = node_params.graph
        nested_output_node_name = node_params.output_node_name
        nested_output_node = nested_graph.get_node_by_name(nested_output_node_name)
        # operation structure map contains inputs to the graph node
        # so that proxy input node can refer to them
        nested_op_structure_info = OperationStructureExtractor(graph=nested_graph).extract(
            node=nested_output_node,
            proxy_input_operation_structures=input_operation_structures,
            keep_all_source_columns=global_state.keep_all_source_columns,
        )
        nested_operation_structure = nested_op_structure_info.operation_structure_map[
            nested_output_node_name
        ]
        return self._prepare_operation_structure(
            node=node,
            operation_structure=nested_operation_structure,
            proxy_input_operation_structures=input_operation_structures,
        )

    def _post_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OperationStructureInfo,
        node: Node,
        inputs: List[OperationStructure],
        skip_post: bool,
    ) -> OperationStructure:
        if node.name in global_state.operation_structure_map:
            return global_state.operation_structure_map[node.name]

        if isinstance(node, BaseGraphNode):
            operation_structure = self._derive_nested_graph_operation_structure(
                node=node,
                input_operation_structures=inputs,
                global_state=global_state,
            )
        else:
            operation_structure = node.derive_node_operation_info(
                inputs=inputs,
                global_state=global_state,
            )

        global_state.operation_structure_map[node.name] = operation_structure
        return operation_structure

    def extract(
        self,
        node: Node,
        proxy_input_operation_structures: Optional[List[OperationStructure]] = None,
        **kwargs: Any,
    ) -> OperationStructureInfo:
        state_params: Dict[str, Any] = {"keep_all_source_columns": True}
        if "keep_all_source_columns" in kwargs:
            # if this parameter is set, then the operation structure will keep all the source columns
            # even if they are not directly used in the operation (for example, event timestamp & entity columns
            # used in group by node)
            state_params["keep_all_source_columns"] = kwargs["keep_all_source_columns"]
        if proxy_input_operation_structures:
            state_params["proxy_input_operation_structures"] = proxy_input_operation_structures

        global_state = OperationStructureInfo(**state_params)
        self._extract(
            node=node,
            branch_state=FeatureByteBaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        return global_state
