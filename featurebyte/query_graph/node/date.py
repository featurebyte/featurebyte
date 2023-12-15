"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Sequence, Tuple, Union

from pydantic import BaseModel, Field

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithSingleOperandNode,
)
from featurebyte.query_graph.node.metadata.config import OnDemandViewCodeGenConfig, SDKCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import NodeOutputCategory, OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationContext,
    ExpressionStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
    get_object_class_from_function_call,
)


class DatetimeExtractNode(BaseSeriesOutputNode):
    """DatetimeExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: DatetimeSupportedPropertyType
        timezone_offset: Optional[str]

    type: Literal[NodeType.DT_EXTRACT] = Field(NodeType.DT_EXTRACT, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        ts_operand: str = var_name_expressions[0].as_input()

        offset_operand: Optional[str]
        if self.parameters.timezone_offset is not None:
            offset_operand = ValueStr.create(self.parameters.timezone_offset).as_input()
        elif len(var_name_expressions) == 2:
            offset_operand = var_name_expressions[1].as_input()
        else:
            offset_operand = None

        date_property: str = self.parameters.property
        if date_property == "dayofweek":
            date_property = "day_of_week"

        output: VarNameExpressionInfo
        if offset_operand is None:
            output = VariableNameStr(f"{ts_operand}.dt.{date_property}")
        else:
            output = ExpressionStr(f"{ts_operand}.dt.tz_offset({offset_operand}).{date_property}")

        return [], output

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        ts_operand: str = var_name_expressions[0].as_input()

        statements: List[StatementT] = []
        offset_operand: Optional[Union[str, VariableNameStr]]
        if self.parameters.timezone_offset is not None:
            delta_val = f"{self.parameters.timezone_offset}:00"
            delta = get_object_class_from_function_call("pd.to_timedelta", delta_val)
            offset_operand = var_name_generator.convert_to_variable_name(
                variable_name_prefix="tz_offset", node_name=None
            )
            statements.append((offset_operand, delta))
        elif len(var_name_expressions) == 2:
            offset_operand = var_name_expressions[1].as_input()
        else:
            offset_operand = None

        dt_var_name: Union[str, VariableNameStr]
        if offset_operand:
            dt_var_name = var_name_generator.convert_to_variable_name(
                variable_name_prefix="feat_dt", node_name=None
            )
            expr = ExpressionStr(f"{ts_operand} + {offset_operand}")
            statements.append((dt_var_name, expr))
        else:
            dt_var_name = ts_operand

        output = ExpressionStr(f"{dt_var_name}.dt.{self.parameters.property}")
        return statements, output


class TimeDeltaExtractNode(BaseSeriesOutputWithSingleOperandNode):
    """TimeDeltaExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA_EXTRACT] = Field(NodeType.TIMEDELTA_EXTRACT, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.dt.{self.parameters.property}"

    def generate_odfv_expression(self, operand: str) -> str:
        if self.parameters.property == "millisecond":
            return f"{operand}.dt.microseconds // 1000"
        if self.parameters.property == "microsecond":
            return f"{operand}.dt.microseconds"
        if self.parameters.property == "second":
            return f"{operand}.dt.seconds"

        unit_to_seconds = {
            "day": 24 * 60 * 60,
            "hour": 60 * 60,
            "minute": 60,
        }
        return f"{operand}.dt.seconds // {unit_to_seconds[self.parameters.property]}"


class DateDifferenceNode(BaseSeriesOutputNode):
    """DateDifferenceNode class"""

    type: Literal[NodeType.DATE_DIFF] = Field(NodeType.DATE_DIFF, const=True)

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.TIMEDELTA

    def _derive_python_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if len(node_inputs) == 1:
            # we don't allow subtracting timestamp with a scalar timedelta through SDK
            raise RuntimeError("DateAddNode with only one input is not supported")

        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand = var_name_expressions[0].as_input()
        right_operand = var_name_expressions[1].as_input()
        return [], ExpressionStr(f"{left_operand} - {right_operand}")

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, operation_structure, config, context
        return self._derive_python_code(node_inputs)

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs)


class TimeDeltaNode(BaseSeriesOutputNode):
    """TimeDeltaNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        unit: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA] = Field(NodeType.TIMEDELTA, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.TIMEDELTA

    def _derive_python_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        node_output_type: NodeOutputType,
        node_output_category: NodeOutputCategory,
        timedelta_func: Union[ClassEnum, str],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = var_name_expressions[0]
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=node_output_type,
            node_output_category=node_output_category,
            node_name=self.name,
        )
        if isinstance(timedelta_func, ClassEnum):
            obj = timedelta_func(var_name_expression, unit=self.parameters.unit)
        else:
            obj = get_object_class_from_function_call(
                timedelta_func, var_name_expression, unit=self.parameters.unit
            )
        statements.append((var_name, obj))
        return statements, var_name

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config, context
        return self._derive_python_code(
            node_inputs,
            var_name_generator,
            operation_structure.output_type,
            operation_structure.output_category,
            ClassEnum.TO_TIMEDELTA,
        )

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = config
        return self._derive_python_code(
            node_inputs,
            var_name_generator,
            NodeOutputType.SERIES,
            NodeOutputCategory.FEATURE,
            "pd.to_timedelta",
        )


class DateAddNode(BaseSeriesOutputNode):
    """DateAddNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[int]

    type: Literal[NodeType.DATE_ADD] = Field(NodeType.DATE_ADD, const=True)
    parameters: Parameters

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        if inputs[0].output_category == NodeOutputCategory.FEATURE:
            # when the inputs[0] is requested column, inputs[0].columns is empty.
            # in this case, we should derive the var type from inputs[0].aggregations
            return inputs[0].aggregations[0].dtype
        return inputs[0].columns[0].dtype

    def _derive_python_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if len(node_inputs) == 1:
            # we don't allow adding timestamp with a scalar timedelta through SDK
            raise RuntimeError("DateAddNode with only one input is not supported")

        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand: str = var_name_expressions[0].as_input()
        right_operand = var_name_expressions[1].as_input()
        return [], ExpressionStr(f"{left_operand} + {right_operand}")

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: SDKCodeGenConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, operation_structure, config, context
        return self._derive_python_code(node_inputs)

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        _ = var_name_generator, config
        return self._derive_python_code(node_inputs)
