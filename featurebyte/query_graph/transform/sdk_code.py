"""
This module contains an extractor class to generate SDK codes from a query graph.
"""
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerationConfig,
    CodeGenerator,
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
    code_generation_config: CodeGenerationConfig
        Code generation configuration
    var_name_generator: VariableNameGenerator
        Variable name generator is used to generate variable names used in the generated SDK codes
    code_generator: CodeGenerator
        Code generator is used to generate final SDK codes from a list of statements & imports
    """

    node_name_to_post_compute_output: Dict[str, VarNameExpNodeType] = Field(default_factory=dict)
    node_name_to_operation_structure: Dict[str, OperationStructure] = Field(default_factory=dict)
    code_generation_config: CodeGenerationConfig = Field(default_factory=CodeGenerationConfig)
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
        if node.type == NodeType.GRAPH and node.parameters.type == GraphNodeType.ITEM_VIEW:  # type: ignore
            # item view consume 2 inputs (item table & event view), when constructing SDK code, we only need to use the
            # first input (item table).
            return input_node_names[:1], False
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
        inputs: List[VarNameExpNodeType],
        skip_post: bool,
    ) -> VarNameExpNodeType:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        # construct SDK code
        op_struct = global_state.node_name_to_operation_structure[node.name]
        input_var_name_expressions: List[VarNameExpressionStr] = []
        input_node_types: List[NodeType] = []
        for var_name_expr, node_type in inputs:
            input_var_name_expressions.append(var_name_expr)
            input_node_types.append(node_type)

        statements, var_name_or_expr = node.derive_sdk_code(
            input_var_name_expressions=input_var_name_expressions,
            input_node_types=input_node_types,
            var_name_generator=global_state.var_name_generator,
            operation_structure=op_struct,
            config=global_state.code_generation_config,
        )
        global_state.code_generator.add_statements(statements=statements)

        # update global state
        global_state.node_name_to_post_compute_output[node.name] = (var_name_or_expr, node.type)

        # return the output variable name & node type of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr, node.type

    def extract(
        self,
        node: Node,
        to_use_saved_data: bool = False,
        feature_store_name: Optional[str] = None,
        feature_store_id: Optional[ObjectId] = None,
        table_id_to_info: Optional[Dict[ObjectId, Dict[str, Any]]] = None,
        **kwargs: Any,
    ) -> SDKCodeGlobalState:
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)

        code_generation_config: Dict[str, Any] = {"to_use_saved_data": to_use_saved_data}
        if feature_store_name:
            code_generation_config["feature_store_name"] = feature_store_name
        if feature_store_id:
            code_generation_config["feature_store_id"] = feature_store_id
        if table_id_to_info:
            code_generation_config["table_id_to_info"] = table_id_to_info

        global_state = SDKCodeGlobalState(
            node_name_to_operation_structure=op_struct_info.operation_structure_map,
            code_generation_config=CodeGenerationConfig(**code_generation_config),
        )
        var_name_or_expr, _ = self._extract(
            node=node,
            branch_state=BaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        final_output_name = global_state.code_generation_config.final_output_name
        output_var = global_state.var_name_generator.convert_to_variable_name(final_output_name)
        global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])
        return global_state
