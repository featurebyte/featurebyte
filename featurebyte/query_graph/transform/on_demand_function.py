"""
On demand function (for DataBricks) related classes and functions.
"""
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import GraphNodeType, NodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import OnDemandFunctionCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
)
from featurebyte.query_graph.node.nested import (
    BaseGraphNodeParameters,
    OfflineStoreIngestQueryGraphNodeParameters,
)
from featurebyte.query_graph.node.request import RequestColumnNode
from featurebyte.query_graph.transform.base import BaseGraphExtractor


class InputArgumentInfo(BaseModel):
    """
    Input argument info
    """

    py_type: str
    column_name: str


class OnDemandFeatureFunctionGlobalState(BaseModel):
    """
    On demand feature function global state
    """

    node_name_to_post_compute_output: Dict[str, VarNameExpressionInfo] = Field(default_factory=dict)
    input_var_name_to_info: Dict[str, InputArgumentInfo] = Field(default_factory=dict)
    code_generation_config: OnDemandFunctionCodeGenConfig = Field(
        default_factory=OnDemandFunctionCodeGenConfig
    )
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(
        default_factory=lambda: CodeGenerator(template="on_demand_function.tpl")
    )

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

    def generate_code(self) -> str:
        """
        Generate code

        Returns
        -------
        str
            Generated code
        """
        input_arg_signatures = []
        comments = []
        for input_arg_name, input_info in sorted(self.input_var_name_to_info.items()):
            input_arg_signatures.append(f"{input_arg_name}: {input_info.py_type}")
            comments.append(f"# {input_arg_name}: {input_info.column_name}")

        output_type = to_on_demand_function_signature_type(self.code_generation_config.output_dtype)
        return self.code_generator.generate(
            to_format=True,
            function_name=self.code_generation_config.on_demand_function_name,
            input_arguments=", ".join(input_arg_signatures),
            comments="\n".join(comments),
            output_var_name=self.code_generation_config.output_var_name,
            output_type=output_type,
        )


def to_on_demand_function_signature_type(dtype: DBVarType) -> str:
    """
    Convert DBVarType to on demand function input type

    Parameters
    ----------
    dtype: DBVarType
        DBVarType

    Returns
    -------
    str
        On demand function input type in string

    Raises
    ------
    ValueError
        If dtype is not supported
    """
    output = dtype.to_type_str()
    if output:
        return output
    if dtype in DBVarType.supported_timestamp_types():
        return "str"
    if dtype in DBVarType.json_conversion_types():
        return "str"
    raise ValueError(f"Unsupported dtype: {dtype}")


class OnDemandFeatureFunctionExtractor(
    BaseGraphExtractor[
        OnDemandFeatureFunctionGlobalState, BaseModel, OnDemandFeatureFunctionGlobalState
    ]
):
    """
    On demand feature function extractor
    """

    def _pre_compute(
        self,
        branch_state: BaseModel,
        global_state: OnDemandFeatureFunctionGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: BaseModel,
        global_state: OnDemandFeatureFunctionGlobalState,
        node: Node,
        input_node: Node,
    ) -> BaseModel:
        return branch_state

    @staticmethod
    def _check_for_input_argument_registration(
        node: Node, global_state: OnDemandFeatureFunctionGlobalState
    ):
        if node.type == NodeType.GRAPH:
            node_params = node.parameters
            assert isinstance(node_params, BaseGraphNodeParameters)
            if node_params.type == GraphNodeType.OFFLINE_STORE_INGEST_QUERY:
                assert isinstance(node_params, OfflineStoreIngestQueryGraphNodeParameters)
                global_state.register_input_argument(
                    variable_name_prefix=global_state.code_generation_config.input_var_prefix,
                    py_type=to_on_demand_function_signature_type(
                        DBVarType(node_params.output_dtype)
                    ),
                    column_name=node_params.output_column_name,
                )

        if node.type == NodeType.REQUEST_COLUMN:
            assert isinstance(node.parameters, RequestColumnNode.RequestColumnNodeParameters)
            global_state.register_input_argument(
                variable_name_prefix=global_state.code_generation_config.request_input_var_prefix,
                py_type=to_on_demand_function_signature_type(DBVarType(node.parameters.dtype)),
                column_name=node.parameters.column_name,
            )

    def _post_compute(
        self,
        branch_state: BaseModel,
        global_state: OnDemandFeatureFunctionGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> VarNameExpressionInfo:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        statements, var_name_or_expr = node.derive_on_demand_function_code(
            node_inputs=inputs,
            var_name_generator=global_state.var_name_generator,
            config=global_state.code_generation_config,
        )

        # update global state
        global_state.code_generator.add_statements(statements=statements)
        global_state.node_name_to_post_compute_output[node.name] = var_name_or_expr
        self._check_for_input_argument_registration(node=node, global_state=global_state)

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr

    def extract(self, node: Node, **kwargs: Any) -> OnDemandFeatureFunctionGlobalState:
        """
        Extract on demand function

        Parameters
        ----------
        node: Node
            Node

        Returns
        -------
        OnDemandFunctionGlobalState
            On demand function global state
        """
        code_generation_config = OnDemandFunctionCodeGenConfig(**kwargs)
        global_state = OnDemandFeatureFunctionGlobalState(
            code_generation_config=code_generation_config,
            var_name_generator=VariableNameGenerator(one_based=True),
        )
        var_name_or_expr = self._extract(
            node=node,
            branch_state=BaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        output_var = VariableNameStr(global_state.code_generation_config.output_var_name)
        global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])
        return global_state
