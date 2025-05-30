"""
Request data related node classes
"""

import textwrap
from typing import Any, List, Sequence, Tuple

from pydantic import StrictStr, model_validator
from typing_extensions import Literal

from featurebyte.enum import DBVarType, SourceType, SpecialColumnName
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.base import BaseNode
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import (
    AggregationColumn,
    FeatureDataColumnType,
    NodeOutputCategory,
    OperationStructure,
    OperationStructureInfo,
)
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    ExpressionStr,
    NodeCodeGenOutput,
    StatementStr,
    StatementT,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
)
from featurebyte.query_graph.node.utils import (
    get_parse_timestamp_tz_tuple_function_string,
    subset_frame_column_expr,
)


class RequestColumnNode(BaseNode):
    """Request column node used by on-demand features"""

    class RequestColumnNodeParameters(FeatureByteBaseModel):
        """Node parameters"""

        column_name: StrictStr
        dtype: DBVarType  # deprecated, keep it for old client compatibility
        dtype_info: DBVarTypeInfo

        @model_validator(mode="before")
        @classmethod
        def _handle_backward_compatibility_for_dtype_info(
            cls, values: dict[str, Any]
        ) -> dict[str, Any]:
            if values.get("dtype_info") is None and values.get("dtype"):
                # handle backward compatibility old way of specifying dtype
                values["dtype_info"] = DBVarTypeInfo(dtype=DBVarType(values["dtype"]))

            if values.get("dtype") is None and values.get("dtype_info"):
                # handle backward compatibility for graph that breaks old client
                dtype_info = values["dtype_info"]
                if isinstance(dtype_info, dict):
                    values["dtype_info"] = DBVarTypeInfo(**dtype_info)
                values["dtype"] = values["dtype_info"].dtype
            return values

    type: Literal[NodeType.REQUEST_COLUMN] = NodeType.REQUEST_COLUMN
    output_type: NodeOutputType
    parameters: RequestColumnNodeParameters

    @property
    def max_input_count(self) -> int:
        return 0

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _derive_node_operation_info(
        self,
        inputs: List[OperationStructure],
        global_state: OperationStructureInfo,
    ) -> OperationStructure:
        return OperationStructure(
            columns=[],
            aggregations=[
                AggregationColumn(
                    name=self.parameters.column_name,
                    dtype_info=self.parameters.dtype_info,
                    filter=False,
                    node_names={self.name},
                    node_name=self.name,
                    method=None,
                    keys=[],
                    window=None,
                    category=None,
                    offset=None,
                    type=FeatureDataColumnType.AGGREGATION,
                    column=None,
                    aggregation_type=NodeType.REQUEST_COLUMN,
                ),
            ],
            output_type=NodeOutputType.SERIES,
            output_category=NodeOutputCategory.FEATURE,
            row_index_lineage=(self.name,),
        )

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements: List[StatementT] = []
        var_name = var_name_generator.convert_to_variable_name("request_col", node_name=self.name)
        if self.parameters.column_name == SpecialColumnName.POINT_IN_TIME:
            obj = ClassEnum.REQUEST_COLUMN(
                _method_name="point_in_time",
            )
        else:
            raise NotImplementedError("Currently only POINT_IN_TIME column is supported")
        statements.append((var_name, obj))
        return statements, var_name

    def _derive_on_demand_view_or_user_defined_function_helper(
        self,
        var_name_generator: VariableNameGenerator,
        input_var_name_expr: VariableNameStr,
        var_name_prefix: str,
        is_databricks_udf: bool,
        source_type: SourceType,
    ) -> Tuple[List[StatementT], VariableNameStr]:
        if self.parameters.dtype in DBVarType.supported_timestamp_types():
            var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix=var_name_prefix, node_name=self.name
            )
            statement = (
                var_name,
                self._to_datetime_expr(input_var_name_expr, to_handle_none=is_databricks_udf),
            )
            return [statement], var_name
        if self.parameters.dtype == DBVarType.TIMESTAMP_TZ_TUPLE:
            func_name = "parse_timestamp_tz_tuple"
            statements: List[StatementT] = []
            if var_name_generator.should_insert_function(function_name=func_name):
                statements.append(
                    StatementStr(
                        textwrap.dedent(get_parse_timestamp_tz_tuple_function_string(func_name))
                    )
                )

            var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix=var_name_prefix, node_name=self.name
            )
            if is_databricks_udf:
                statements.append((var_name, ExpressionStr(f"{func_name}({input_var_name_expr})")))
            else:
                parse_timestamp_expr = ExpressionStr(f"{input_var_name_expr}.apply({func_name})")
                statements.append((var_name, parse_timestamp_expr))
            return statements, var_name
        return [], input_var_name_expr

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_df_name = config.input_df_name
        column_name = self.parameters.column_name
        expr = VariableNameStr(subset_frame_column_expr(input_df_name, column_name))
        return self._derive_on_demand_view_or_user_defined_function_helper(
            var_name_generator=var_name_generator,
            input_var_name_expr=expr,
            var_name_prefix="request_col",
            is_databricks_udf=False,
            source_type=config.source_type,
        )

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        associated_node_name = None
        if self.parameters.dtype not in DBVarType.supported_timestamp_types():
            associated_node_name = self.name

        request_input_var_name = var_name_generator.convert_to_variable_name(
            variable_name_prefix=config.request_input_var_prefix, node_name=associated_node_name
        )
        return self._derive_on_demand_view_or_user_defined_function_helper(
            var_name_generator=var_name_generator,
            input_var_name_expr=request_input_var_name,
            var_name_prefix="feat",
            is_databricks_udf=True,
            source_type=config.source_type,
        )
