"""
On demand feature view (for Feast) related classes and functions.
"""
from typing import Any, Dict, List, Tuple

from pydantic import BaseModel, Field

from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
)
from featurebyte.query_graph.transform.base import BaseGraphExtractor


class OnDemandFeatureViewGlobalState(BaseModel):
    """
    On demand feature view global state
    """

    node_name_to_post_compute_output: Dict[str, VarNameExpressionInfo] = Field(default_factory=dict)
    code_generation_config: OnDemandViewCodeGenConfig = Field(
        default_factory=OnDemandViewCodeGenConfig
    )
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(
        default_factory=lambda: CodeGenerator(template="on_demand_view.tpl")
    )

    def generate_code(self) -> str:
        """
        Generate code

        Returns
        -------
        str
            Generated code
        """
        return self.code_generator.generate(
            to_format=True,
            function_name=self.code_generation_config.on_demand_function_name,
            input_df_name=self.code_generation_config.input_df_name,
            output_df_name=self.code_generation_config.output_df_name,
        )


class OnDemandFeatureViewExtractor(
    BaseGraphExtractor[OnDemandFeatureViewGlobalState, BaseModel, OnDemandFeatureViewGlobalState]
):
    """
    On demand feature view extractor
    """

    def _pre_compute(
        self,
        branch_state: BaseModel,
        global_state: OnDemandFeatureViewGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: BaseModel,
        global_state: OnDemandFeatureViewGlobalState,
        node: Node,
        input_node: Node,
    ) -> BaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: BaseModel,
        global_state: OnDemandFeatureViewGlobalState,
        node: Node,
        inputs: List[Any],
        skip_post: bool,
    ) -> VarNameExpressionInfo:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        statements, var_name_or_expr = node.derive_on_demand_view_code(
            node_inputs=inputs,
            var_name_generator=global_state.var_name_generator,
            config=global_state.code_generation_config,
        )

        # update global state
        global_state.code_generator.add_statements(statements=statements)
        global_state.node_name_to_post_compute_output[node.name] = var_name_or_expr

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return var_name_or_expr

    def extract(self, node: Node, **kwargs: Any) -> OnDemandFeatureViewGlobalState:
        global_state = OnDemandFeatureViewGlobalState(
            code_generation_config=OnDemandViewCodeGenConfig(**kwargs),
        )
        var_name_or_expr = self._extract(
            node=node,
            branch_state=BaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        output_df_name = global_state.code_generation_config.output_df_name
        output_column_name = self.graph.get_node_output_column_name(node_name=node.name)
        output_var = VariableNameStr(f"{output_df_name}['{output_column_name}']")
        global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])
        return global_state
