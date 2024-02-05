"""
On demand feature view (for Feast) related classes and functions.
"""
from typing import Any, Dict, List, Optional, Tuple

import textwrap

from pydantic import BaseModel, Field

from featurebyte.enum import SpecialColumnName
from featurebyte.query_graph.enum import FEAST_TIMESTAMP_POSTFIX, GraphNodeType
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    ExpressionStr,
    StatementStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
)
from featurebyte.query_graph.node.utils import subset_frame_column_expr
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

    @staticmethod
    def generate_ttl_handling_statements(
        feature_name_version: str,
        input_df_name: str,
        output_df_name: str,
        input_column_expr: str,
        ttl_seconds: int,
        var_name_generator: VariableNameGenerator,
        comment: str = "",
        feature_columns_used_for_ttl: Optional[List[str]] = None,
    ) -> StatementStr:
        """
        Generate time-to-live (TTL) handling statements for the feature or target query graph

        Parameters
        ----------
        feature_name_version: str
            Feature name version
        input_df_name: str
            Input dataframe name
        output_df_name: str
            Output dataframe name
        input_column_expr: str
            Input column expression (to be applied for ttl handling)
        ttl_seconds: int
            Time-to-live (TTL) in seconds
        var_name_generator: VariableNameGenerator
            Variable name generator
        comment: str
            Comment
        feature_columns_used_for_ttl: Optional[List[str]]
            Feature columns used for TTL

        Returns
        -------
        StatementStr
            Generated code
        """
        if feature_columns_used_for_ttl is None:
            feature_columns_used_for_ttl = [feature_name_version]

        # feast.online_response.TIMESTAMP_POSTFIX = "__ts" (from feast/online_response.py)
        # hardcoding the timestamp postfix as we don't want to import feast module here
        ttl_ts_columns = [f"{col}{FEAST_TIMESTAMP_POSTFIX}" for col in feature_columns_used_for_ttl]

        # expressions
        feat_ts_col_name = "_feat_ts_col_name"
        feat_ts_col_map = "_feat_ts_col_map"
        subset_pit_expr = subset_frame_column_expr(
            input_df_name, SpecialColumnName.POINT_IN_TIME.value
        )
        subset_feat_time_col_expr = f"{input_df_name}[{feat_ts_col_name}]"
        subset_output_column_expr = subset_frame_column_expr(output_df_name, feature_name_version)

        # variable names
        req_time_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="request_time", node_name=None
        )
        cutoff_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="cutoff", node_name=None
        )
        feat_time_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="feature_timestamp", node_name=None
        )
        mask_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="mask", node_name=None
        )
        return StatementStr(
            textwrap.dedent(
                f"""
            {comment}
            {req_time_var_name} = pd.to_datetime({subset_pit_expr}, utc=True)
            {cutoff_var_name} = {req_time_var_name} - pd.Timedelta(seconds={ttl_seconds})
            {feat_ts_col_map} = {{}}
            for {feat_ts_col_name} in {ttl_ts_columns}:
                {feat_ts_col_map}[{feat_ts_col_name}] = pd.to_datetime({subset_feat_time_col_expr}, unit="s", utc=True)
            {feat_time_name} = pd.DataFrame({feat_ts_col_map}).max(axis=1)
            {mask_var_name} = ({feat_time_name} >= {cutoff_var_name}) & ({feat_time_name} <= {req_time_var_name})
            {input_column_expr}[~{mask_var_name}] = np.nan
            {subset_output_column_expr} = {input_column_expr}
            {output_df_name}.fillna(np.nan, inplace=True)
            """
            ).strip()
        )

    def extract(self, node: Node, **kwargs: Any) -> OnDemandFeatureViewGlobalState:
        has_ttl = kwargs.get("ttl_seconds", 0)
        feature_name_version = kwargs.get("feature_name_version", None)
        assert feature_name_version is not None, "feature_name_version must be provided"
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
        if has_ttl:
            if isinstance(var_name_or_expr, ExpressionStr):
                input_var_name = global_state.var_name_generator.convert_to_variable_name(
                    variable_name_prefix="feat", node_name=None
                )
                global_state.code_generator.add_statements(
                    statements=[(input_var_name, var_name_or_expr)]
                )
            else:
                input_var_name = var_name_or_expr

            feature_columns_used_for_ttl = [
                node.parameters.output_column_name  # type: ignore[attr-defined]
                for node in self.graph.iterate_sorted_graph_nodes(
                    graph_node_types={GraphNodeType.OFFLINE_STORE_INGEST_QUERY}
                )
            ]
            ttl_statements = self.generate_ttl_handling_statements(
                feature_name_version=feature_name_version,
                input_df_name=global_state.code_generation_config.input_df_name,
                output_df_name=global_state.code_generation_config.output_df_name,
                input_column_expr=input_var_name,
                ttl_seconds=has_ttl,
                var_name_generator=global_state.var_name_generator,
                comment=f"# TTL handling for {feature_name_version}",
                feature_columns_used_for_ttl=feature_columns_used_for_ttl,
            )
            global_state.code_generator.add_statements(statements=[ttl_statements])
        else:
            output_var = VariableNameStr(
                subset_frame_column_expr(output_df_name, feature_name_version)
            )
            global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])

        return global_state
