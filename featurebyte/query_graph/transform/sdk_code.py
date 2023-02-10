"""
This module contains an extractor class to generate SDK codes from a query graph.
"""
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    StyleConfig,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpression,
)
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor

# (variable or expression, node_type) tuple is the output type of `_post_compute`,
# it is used to transfer the required info between two consecutive query graph nodes.
# NodeType is required to handle item groupby special case.
VarNameExpNodeType = Tuple[VarNameExpression, NodeType]


class SDKCodeBranchState(BaseModel):
    """SDKCodeBranchState class"""


class SDKCodeGlobalState(BaseModel):
    """SDKCodeGlobalState class"""

    style_config: StyleConfig = Field(default_factory=StyleConfig)
    operation_structure_map: Dict[str, OperationStructure] = Field(default_factory=dict)
    node_name_to_post_compute_output: Dict[str, VarNameExpNodeType] = Field(default_factory=dict)
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(default_factory=CodeGenerator)


class SDKCodeExtractor(
    BaseGraphExtractor[SDKCodeGlobalState, SDKCodeBranchState, SDKCodeGlobalState]
):
    """SDKCodeExtractor class"""

    def _pre_compute(
        self,
        branch_state: SDKCodeBranchState,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: SDKCodeBranchState,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node: Node,
    ) -> SDKCodeBranchState:
        return branch_state

    def _post_compute(
        self,
        branch_state: SDKCodeBranchState,
        global_state: SDKCodeGlobalState,
        node: Node,
        inputs: List[Tuple[str, NodeType]],
        skip_post: bool,
    ) -> VarNameExpNodeType:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        # construct SDK code
        op_struct = global_state.operation_structure_map[node.name]
        input_var_name_expressions, input_node_types = [], []
        if inputs:
            input_var_name_expressions, input_node_types = zip(*inputs)

        sdk_codes, var_name_or_expr = node.derive_sdk_codes(
            input_var_name_expressions=input_var_name_expressions,
            input_node_types=input_node_types,
            var_name_generator=global_state.var_name_generator,
            operation_structure=op_struct,
            style_config=global_state.style_config,
        )
        global_state.code_generator.add_statements(sdk_codes)

        # update global state
        global_state.node_name_to_post_compute_output[node.name] = (var_name_or_expr, node.type)

        # return the output variable name & node type of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr, node.type

    def extract(self, node: Node, **kwargs: Any) -> SDKCodeGlobalState:
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)
        global_state = SDKCodeGlobalState(
            operation_structure_map=op_struct_info.operation_structure_map
        )
        var_name_or_expr, _ = self._extract(
            node=node,
            branch_state=SDKCodeBranchState(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        global_state.code_generator.add_statements([(VariableNameStr("output"), var_name_or_expr)])
        return global_state
