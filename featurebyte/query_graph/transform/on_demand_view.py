"""
On demand feature view (for Feast) related classes and functions.
"""

import json
import textwrap
from typing import Any, Dict, List, Optional, Tuple

from pydantic import Field

from featurebyte.enum import SpecialColumnName
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import FEAST_TIMESTAMP_POSTFIX
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    CodeGenerator,
    NodeCodeGenOutput,
    StatementStr,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
)
from featurebyte.query_graph.node.utils import subset_frame_column_expr
from featurebyte.query_graph.transform.base import BaseGraphExtractor
from featurebyte.query_graph.transform.operation_structure import OperationStructureExtractor
from featurebyte.typing import Scalar


class OnDemandFeatureViewGlobalState(FeatureByteBaseModel):
    """
    On demand feature view global state
    """

    node_name_to_operation_structure: Dict[str, OperationStructure]
    node_name_to_post_compute_output: Dict[str, NodeCodeGenOutput] = Field(default_factory=dict)
    code_generation_config: OnDemandViewCodeGenConfig = Field(
        default_factory=OnDemandViewCodeGenConfig
    )
    var_name_generator: VariableNameGenerator = Field(default_factory=VariableNameGenerator)
    code_generator: CodeGenerator = Field(
        default_factory=lambda: CodeGenerator(template="on_demand_view.tpl")
    )


class OnDemandFeatureViewExtractor(
    BaseGraphExtractor[
        OnDemandFeatureViewGlobalState, FeatureByteBaseModel, OnDemandFeatureViewGlobalState
    ]
):
    """
    On demand feature view extractor
    """

    def _pre_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OnDemandFeatureViewGlobalState,
        node: Node,
        input_node_names: List[str],
    ) -> Tuple[List[str], bool]:
        return input_node_names, False

    def _in_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OnDemandFeatureViewGlobalState,
        node: Node,
        input_node: Node,
    ) -> FeatureByteBaseModel:
        return branch_state

    def _post_compute(
        self,
        branch_state: FeatureByteBaseModel,
        global_state: OnDemandFeatureViewGlobalState,
        node: Node,
        inputs: List[NodeCodeGenOutput],
        skip_post: bool,
    ) -> NodeCodeGenOutput:
        if node.name in global_state.node_name_to_post_compute_output:
            return global_state.node_name_to_post_compute_output[node.name]

        statements, var_name_or_expr = node.derive_on_demand_view_code(
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

        # return the output variable name or expression of current operation so that
        # it can be passed as `inputs` to the next node's post compute operation
        return post_compute_output

    @staticmethod
    def generate_ttl_handling_statements(
        feature_name_version: str,
        input_df_name: str,
        output_df_name: str,
        ttl_seconds: int,
        var_name_generator: VariableNameGenerator,
        cron_expression: Optional[str] = None,
        cron_timezone: Optional[str] = None,
        comment: str = "",
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
        ttl_seconds: int
            Time-to-live (TTL) in seconds
        var_name_generator: VariableNameGenerator
            Variable name generator
        cron_expression: Optional[str]
            Cron expression
        cron_timezone: Optional[str]
            Cron timezone
        comment: str
            Comment

        Returns
        -------
        StatementStr
            Generated code
        """
        # feast.online_response.TIMESTAMP_POSTFIX = "__ts" (from feast/online_response.py)
        # hardcoding the timestamp postfix as we don't want to import feast module here
        ttl_ts_column = f"{feature_name_version}{FEAST_TIMESTAMP_POSTFIX}"
        subset_feat_time_col_expr = subset_frame_column_expr(input_df_name, ttl_ts_column)
        feat_time_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="feature_timestamp", node_name=None
        )
        mask_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix="mask", node_name=None
        )
        subset_output_column_expr = subset_frame_column_expr(output_df_name, feature_name_version)
        input_column_expr = subset_frame_column_expr(input_df_name, feature_name_version)

        if cron_expression:
            assert cron_timezone is not None, "cron_timezone must be provided"
            cron_var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix="cron", node_name=None
            )
            prev_time_var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix="prev_time", node_name=None
            )
            return StatementStr(
                textwrap.dedent(
                    f"""

                    {comment}
                    {cron_var_name} = croniter.croniter({json.dumps(cron_expression)})
                    {prev_time_var_name} = {cron_var_name}.timestamp_to_datetime({cron_var_name}.get_prev())
                    {prev_time_var_name} = {prev_time_var_name}.replace(tzinfo=ZoneInfo({json.dumps(cron_timezone)})).astimezone(pytz.utc)
                    {feat_time_name} = pd.to_datetime({subset_feat_time_col_expr}, unit="s", utc=True)
                    {mask_var_name} = {feat_time_name} <= {prev_time_var_name}
                    {input_df_name}.loc[{mask_var_name}, {repr(feature_name_version)}] = np.nan
                    {subset_output_column_expr} = {input_column_expr}
                    {output_df_name}.fillna(np.nan, inplace=True)

                """
                )
            )
        else:
            # expressions
            subset_pit_expr = subset_frame_column_expr(
                input_df_name, SpecialColumnName.POINT_IN_TIME.value
            )

            # variable names
            req_time_var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix="request_time", node_name=None
            )
            cutoff_var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix="cutoff", node_name=None
            )
            return StatementStr(
                textwrap.dedent(
                    f"""

                {comment}
                {req_time_var_name} = pd.to_datetime({subset_pit_expr}, utc=True)
                {cutoff_var_name} = {req_time_var_name} - pd.Timedelta(seconds={ttl_seconds})
                {feat_time_name} = pd.to_datetime({subset_feat_time_col_expr}, unit="s", utc=True)
                {mask_var_name} = ({feat_time_name} >= {cutoff_var_name}) & ({feat_time_name} <= {req_time_var_name})
                {input_df_name}.loc[~{mask_var_name}, {repr(feature_name_version)}] = np.nan
                {subset_output_column_expr} = {input_column_expr}
                {output_df_name}.fillna(np.nan, inplace=True)

                """
                ).strip()
            )

    @staticmethod
    def generate_null_filling_statements(
        feature_name_version: str,
        output_df_name: str,
        input_column_expr: str,
        fill_value: Scalar,
    ) -> StatementStr:
        """
        Generate null filling statements for the feature or target query graph

        Parameters
        ----------
        feature_name_version: str
            Feature name version
        output_df_name: str
            Output dataframe name
        input_column_expr: str
            Input column expression (to be applied for null filling)
        fill_value: Scalar
            Fill value

        Returns
        -------
        StatementStr
            Generated code
        """
        # expressions
        subset_output_column_expr = subset_frame_column_expr(output_df_name, feature_name_version)
        fill_value_expr = ValueStr(fill_value).as_input()
        return StatementStr(
            f"{subset_output_column_expr} = {input_column_expr}.fillna({fill_value_expr})"
        )

    def extract(self, node: Node, **kwargs: Any) -> OnDemandFeatureViewGlobalState:
        feature_name_version = kwargs.get("feature_name_version", None)
        assert feature_name_version is not None, "feature_name_version must be provided"
        op_struct_info = OperationStructureExtractor(graph=self.graph).extract(node=node)
        global_state = OnDemandFeatureViewGlobalState(
            node_name_to_operation_structure=op_struct_info.operation_structure_map,
            code_generation_config=OnDemandViewCodeGenConfig(**kwargs),
        )
        node_codegen_output = self._extract(
            node=node,
            branch_state=FeatureByteBaseModel(),
            global_state=global_state,
            topological_order_map=self.graph.node_topological_order_map,
        )
        var_name_or_expr = node_codegen_output.var_name_or_expr
        output_df_name = global_state.code_generation_config.output_df_name
        output_var = VariableNameStr(subset_frame_column_expr(output_df_name, feature_name_version))
        global_state.code_generator.add_statements(statements=[(output_var, var_name_or_expr)])
        return global_state
