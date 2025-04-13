"""
On demand function (for DataBricks) related classes and functions.
"""

from typing import Any, Dict, List, Tuple

from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import OnDemandFunctionCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    NodeCodeGenOutput,
    StatementStr,
    VariableNameGenerator,
)
from featurebyte.query_graph.node.nested import (
    BaseGraphNodeParameters,
    OfflineStoreIngestQueryGraphNodeParameters,
)
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor


class InputArgumentInfo(FeatureByteBaseModel):
    """
    Input argument info
    """

    py_type: str
    column_name: str


class SQLInputArgumentInfo(InputArgumentInfo):
    """
    SQL input argument info
    """

    sql_input_var_name: str
    py_input_var_name: str


class OnDemandFeatureFunctionGlobalState(FeatureByteBaseModel):
    """
    On demand feature function global state
    """

    node_name_to_operation_structure: Dict[str, OperationStructure]
    node_name_to_post_compute_output: Dict[str, NodeCodeGenOutput] = Field(default_factory=dict)
    input_var_name_to_info: Dict[str, InputArgumentInfo] = Field(default_factory=dict)
    code_generation_config: OnDemandFunctionCodeGenConfig = Field(
        default_factory=OnDemandFunctionCodeGenConfig
    )
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(
        default_factory=lambda: CodeGenerator(template="on_demand_function.tpl")
    )

    @property
    def sql_inputs_info(self) -> List[SQLInputArgumentInfo]:
        """
        SQL inputs info

        Returns
        -------
        List[SQLInputArgumentInfo]
            SQL input variable name to info
        """
        output = []
        codegen_config = self.code_generation_config
        var_name_generator = VariableNameGenerator(one_based=True)
        for input_arg_name, input_info in sorted(self.input_var_name_to_info.items()):
            sql_param = var_name_generator.convert_to_variable_name(
                variable_name_prefix=codegen_config.get_sql_param_prefix(input_arg_name),
                node_name=None,
            )
            sql_param_info = SQLInputArgumentInfo(
                py_type=input_info.py_type,
                column_name=input_info.column_name,
                py_input_var_name=input_arg_name,
                sql_input_var_name=sql_param,
            )
            output.append(sql_param_info)
        return output

    def register_input_argument(
        self, variable_name_prefix: str, py_type: str, column_name: str
    ) -> None:
        """
        Register input argument

        Parameters
        ----------
        variable_name_prefix: str
            Node name to register the input & output var name
        py_type: str
            Python type in string
        column_name: str
            Expected column name to be used in the generated code
        """
        var_name = self.var_name_generator.get_latest_variable_name(
            variable_name_prefix=variable_name_prefix
        )
        self.input_var_name_to_info[var_name] = InputArgumentInfo(
            py_type=py_type, column_name=column_name
        )

    def generate_code(self, to_sql: bool = False) -> str:
        """
        Generate code

        Parameters
        ----------
        to_sql: bool
            Whether to generate SQL code (True) or Python code (False)

        Returns
        -------
        str
            Generated code
        """
        codegen_config = self.code_generation_config
        sql_func_params = []
        py_func_params = []
        py_comments = []
        input_arg_names = []
        for sql_input_info in self.sql_inputs_info:
            input_arg_name = sql_input_info.py_input_var_name
            sql_type = codegen_config.to_sql_type(sql_input_info.py_type)
            sql_func_params.append(f"{sql_input_info.sql_input_var_name} {sql_type}")
            py_func_params.append(f"{input_arg_name}: {sql_input_info.py_type}")
            py_comments.append(f"# {input_arg_name}: {sql_input_info.column_name}")
            input_arg_names.append(sql_input_info.sql_input_var_name)

        function_codes = self.code_generator.generate(
            to_format=True,
            to_remove_unused_variables=True,
            py_function_name=codegen_config.function_name,
            py_function_params=", ".join(py_func_params),
            py_return_type=codegen_config.return_type,
            py_comment="\n".join(py_comments),
        )

        if to_sql:
            code_generator = CodeGenerator(template="on_demand_function_sql.tpl")
            # since the final code is SQL, do not use python formatter or remove unused variables
            return code_generator.generate(
                to_format=False,
                to_remove_unused_variables=False,
                sql_function_name=codegen_config.sql_function_name,
                sql_function_params=", ".join(sql_func_params),
                sql_return_type=codegen_config.sql_return_type,
                sql_comment=codegen_config.sql_comment,
                input_arguments=", ".join(input_arg_names),
                py_function_name=codegen_config.function_name,
                py_function_body=function_codes.strip(),
            )

        return function_codes


class OnDemandFeatureFunctionExtractor(
    BaseGraphExtractor[
        OnDemandFeatureFunctionGlobalState, FeatureByteBaseModel, OnDemandFeatureFunctionGlobalState
    ]
):
    """
    On demand feature function extractor
    """

    def _pre_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OnDemandFeatureFunctionGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OnDemandFeatureFunctionGlobalState,
        node: Node,
        input_node: Node,
    ) -> FeatureByteBaseModel:
        return branch_state

    @staticmethod
    def _check_for_input_argument_registration(
        node: Node, global_state: OnDemandFeatureFunctionGlobalState
    ) -> None:
        codegen_config = global_state.code_generation_config
        if node.type == NodeType.GRAPH:
            node_params = node.parameters
            assert isinstance(node_params, BaseGraphNodeParameters)
            if node_params.type == GraphNodeType.OFFLINE_STORE_INGEST_QUERY:
                assert isinstance(node_params, OfflineStoreIngestQueryGraphNodeParameters)
                global_state.register_input_argument(
                    variable_name_prefix=codegen_config.input_var_prefix,
                    py_type=codegen_config.to_py_type(DBVarType(node_params.output_dtype)),
                    column_name=node_params.output_column_name,
                )

        if node.type == NodeType.REQUEST_COLUMN:
            assert isinstance(node.parameters, RequestColumnNode.RequestColumnNodeParameters)
            global_state.register_input_argument(
                variable_name_prefix=codegen_config.request_input_var_prefix,
                py_type=codegen_config.to_py_type(DBVarType(node.parameters.dtype)),
                column_name=node.parameters.column_name,
            )

    def _post_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OnDemandFeatureFunctionGlobalState,
        node: Node,
        inputs: List[NodeCodeGenOutput],
        skip_post: bool,
    ) -> NodeCodeGenOutput:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        statements, var_name_or_expr = node.derive_user_defined_function_code(
            node_inputs=inputs,
            var_name_generator=global_state.var_name_generator,
            config=global_state.code_generation_config,
        )

        # update global state
        post_compute_output = NodeCodeGenOutput(
            var_name_or_expr=var_name_or_expr,
            operation_structure=global_state.node_name_to_operation_structure[node.name],
        )
        global_state.code_generator.add_statements(statements=statements)
        global_state.node_name_to_post_compute_output[node.name] = post_compute_output
        self._check_for_input_argument_registration(node=node, global_state=global_state)

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return post_compute_output

    def extract(self, node: Node, **kwargs: Any) -> OnDemandFeatureFunctionGlobalState:
        """
        Extract on demand function

        Parameters
        ----------
        node: Node
            Node
        **kwargs: Any
            Keyword arguments

        Returns
        -------
        OnDemandFeatureFunctionGlobalState
            On demand function global state
        """
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)
        code_generation_config = OnDemandFunctionCodeGenConfig(**kwargs)
        global_state = OnDemandFeatureFunctionGlobalState(
            node_name_to_operation_structure=op_struct_info.operation_structure_map,
            code_generation_config=code_generation_config,
            var_name_generator=VariableNameGenerator(one_based=True),
        )
        node_code_gen_output = self._extract(
            node=node,
            branch_state=FeatureByteBaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        global_state.code_generator.add_statements(
            statements=[StatementStr(f"return {node_code_gen_output.var_name_or_expr}")],
        )
        return global_state
