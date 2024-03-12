"""
Request data related node classes
"""
from typing import List, Literal, Sequence, Tuple

from pydantic import BaseModel, Field, StrictStr

from featurebyte.enum import DBVarType, SpecialColumnName
from featurebyte.query_graph.enum import NodeOutputType, NodeType
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
    StatementT,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    get_object_class_from_function_call,
)
from featurebyte.query_graph.node.utils import subset_frame_column_expr


class RequestColumnNode(BaseNode):
    """Request column node used by on-demand features"""

    class RequestColumnNodeParameters(BaseModel):
        """Node parameters"""

        column_name: StrictStr
        dtype: DBVarType

    type: Literal[NodeType.REQUEST_COLUMN] = Field(NodeType.REQUEST_COLUMN, const=True)
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
                    dtype=self.parameters.dtype,
                    filter=False,
                    node_names={self.name},
                    node_name=self.name,
                    method=None,
                    keys=[],
                    window=None,
                    category=None,
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
        node_inputs: List[VarNameExpressionInfo],
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
        input_var_name_expr: VarNameExpressionInfo,
        var_name_prefix: str,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if self.parameters.dtype in DBVarType.supported_timestamp_types():
            var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix=var_name_prefix, node_name=self.name
            )
            expression = get_object_class_from_function_call(
                "pd.to_datetime", input_var_name_expr, utc=True
            )
            return [(var_name, expression)], var_name
        return [], input_var_name_expr

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
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
        )

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
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
        )
