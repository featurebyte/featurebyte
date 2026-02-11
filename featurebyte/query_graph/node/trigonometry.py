"""
Trigonometry node module
"""

from typing import List, Sequence, Tuple

from typing_extensions import Literal

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
    SDKCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    ExpressionStr,
    NodeCodeGenOutput,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class Atan2Node(BaseSeriesOutputNode):
    """Atan2Node class - computes the two-argument arctangent"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

    type: Literal[NodeType.ATAN2] = NodeType.ATAN2
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.FLOAT)

    def _derive_sdk_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )
        obj = ClassEnum.ATAN2(
            y=var_name_expressions[0],
            x=var_name_expressions[1],
        )
        statements.append((var_name, obj))
        return statements, var_name

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        y_expr = input_var_name_expressions[0]
        x_expr = input_var_name_expressions[1]
        result_expr = ExpressionStr(f"np.arctan2({y_expr}, {x_expr})")
        return [], result_expr

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[NodeCodeGenOutput],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        y_expr = input_var_name_expressions[0]
        x_expr = input_var_name_expressions[1]
        result_expr = ExpressionStr(f"np.arctan2({y_expr}, {x_expr})")
        return [], result_expr
