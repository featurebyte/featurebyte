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
    VarNameExpressionStr,
)
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor

# (variable or expression, node_type) tuple is the output type of `_post_compute`,
# it is used to transfer the required info between two consecutive query graph nodes.
# NodeType is required to handle item groupby special case.
VarNameExpNodeType = Tuple[VarNameExpressionStr, NodeType]


class SDKCodeGlobalState(BaseModel):
    """
    SDKCodeGlobalState class

    node_name_to_post_compute_output: Dict[str, VarNameExpNodeType]
        Cache to store node name to _post_compute_output (to remove redundant graph node traversals)
    node_name_to_operation_structure: Dict[str, OperationStructure]
        Operation structure mapping for each node in the graph
    style_config: StyleConfig
        Configuration to control style of generated codes (for example, maximum expression width)
    var_name_generator: VariableNameGenerator
        Variable name generator is used to generate variable names used in the generated SDK codes
    code_generator: CodeGenerator
        Code generator is used to generate final SDK codes from a list of statements & imports
    """

    node_name_to_post_compute_output: Dict[str, VarNameExpNodeType] = Field(default_factory=dict)
    node_name_to_operation_structure: Dict[str, OperationStructure] = Field(default_factory=dict)
    style_config: StyleConfig = Field(default_factory=StyleConfig)
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(default_factory=CodeGenerator)


class SDKCodeExtractor(BaseGraphExtractor[SDKCodeGlobalState, BaseModel, SDKCodeGlobalState]):
    """
    SDKCodeExtractor class is responsible to generate SDK codes from a given query graph.
    """

    def _pre_compute(
        self,
        branch_state: BaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: BaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        input_node: Node,
    ) -> BaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: BaseModel,
        global_state: SDKCodeGlobalState,
        node: Node,
        inputs: List[Tuple[str, NodeType]],
        skip_post: bool,
    ) -> VarNameExpNodeType:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        # construct SDK code
        op_struct = global_state.node_name_to_operation_structure[node.name]
        input_var_name_expressions: List[VarNameExpressionStr] = []
        input_node_types: List[NodeType] = []
        if inputs:
            input_var_name_expressions, input_node_types = zip(*inputs)

        statements, var_name_or_expr = node.derive_sdk_codes(
            input_var_name_expressions=input_var_name_expressions,
            input_node_types=input_node_types,
            var_name_generator=global_state.var_name_generator,
            operation_structure=op_struct,
            style_config=global_state.style_config,
        )
        global_state.code_generator.add_statements(statements=statements)

        # update global state
        global_state.node_name_to_post_compute_output[node.name] = (var_name_or_expr, node.type)

        # return the output variable name & node type of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr, node.type

    def extract(self, node: Node, **kwargs: Any) -> SDKCodeGlobalState:
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)
        global_state = SDKCodeGlobalState(
            node_name_to_operation_structure=op_struct_info.operation_structure_map
        )
        var_name_or_expr, _ = self._extract(
            node=node,
            branch_state=BaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        final_output_name = global_state.style_config.final_output_name
        output_var = global_state.var_name_generator.convert_to_variable_name(final_output_name)
        global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])
        return global_state
