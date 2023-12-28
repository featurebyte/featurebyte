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
    VarNameExpressionInfo,
)
from featurebyte.query_graph.node.nested import (
    BaseGraphNodeParameters,
    OfflineStoreIngestQueryGraphNodeParameters,
)
from featurebyte.query_graph.transform.base import BaseGraphExtractor, BranchStateT, GlobalStateT


class OnDemandFeatureFunctionGlobalState(BaseModel):
    """
    On demand feature function global state
    """

    node_name_to_post_compute_output: Dict[str, VarNameExpressionInfo] = Field(default_factory=dict)
    io_var_name_to_type: Dict[str, str] = Field(default_factory=dict)
    code_generation_config: OnDemandFunctionCodeGenConfig = Field(
        default_factory=OnDemandFunctionCodeGenConfig
    )
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(
        default_factory=lambda: CodeGenerator(template="on_demand_function.tpl")
    )

    def register_input_argument(self, node_name: str, io_type: str) -> None:
        """
        Register input argument

        Parameters
        ----------
        node_name: str
            Node name to register the input & output var name
        io_type: str
            IO type
        """
        io_var_name = self.var_name_generator.node_name_to_var_name[node_name]
        self.io_var_name_to_type[io_var_name] = io_type

    def generate_code(self) -> str:
        """
        Generate code

        Returns
        -------
        str
            Generated code
        """
        input_args_signature = ", ".join(
            sorted(
                [
                    f"{io_var_name}: {io_type}"
                    for io_var_name, io_type in self.io_var_name_to_type.items()
                    if io_var_name.startswith(self.code_generation_config.input_var_prefix)
                ]
            )
        )
        return self.code_generator.generate(
            to_format=True,
            function_name=self.code_generation_config.on_demand_function_name,
            input_arguments=input_args_signature,
            output_var_name=self.code_generation_config.output_var_name,
        )


def to_on_demand_function_input_type(dtype: DBVarType) -> str:
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
    if dtype in DBVarType.json_conversion_types():
        return "str"
    raise ValueError(f"Unsupported dtype: {dtype}")


class OnDemandFunctionExtractor(
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
        self, branch_state: BranchStateT, global_state: GlobalStateT, node: Node, input_node: Node
    ) -> BranchStateT:
        return branch_state

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

        if node.type == NodeType.GRAPH:
            node_params = node.parameters
            assert isinstance(node_params, BaseGraphNodeParameters)
            if node_params.type == GraphNodeType.OFFLINE_STORE_INGEST_QUERY:
                assert isinstance(node_params, OfflineStoreIngestQueryGraphNodeParameters)
                global_state.register_input_argument(
                    node_name=node.name,
                    io_type=to_on_demand_function_input_type(node_params.output_dtype),
                )

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr
