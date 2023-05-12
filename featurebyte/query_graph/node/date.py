"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Sequence, Tuple

from pydantic import BaseModel, Field

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithSingleOperandNode,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ClassEnum,
    CodeGenerationConfig,
    CodeGenerationContext,
    ExpressionStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionInfo,
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
        config: CodeGenerationConfig,
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


class DateDifference(BaseSeriesOutputNode):
    """DateDifference class"""

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

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand: str = var_name_expressions[0].as_input()
        right_operand = var_name_expressions[1].as_input()
        return [], ExpressionStr(f"{left_operand} - {right_operand}")


class TimeDelta(BaseSeriesOutputNode):
    """TimeDelta class"""

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

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        var_name_expression = var_name_expressions[0]
        statements: List[StatementT] = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
            node_name=self.name,
        )
        obj = ClassEnum.TO_TIMEDELTA(series=var_name_expression, unit=self.parameters.unit)
        statements.append((var_name, obj))
        return statements, var_name


class DateAdd(BaseSeriesOutputNode):
    """DateAdd class"""

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
        return inputs[0].columns[0].dtype

    def _derive_sdk_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
        context: CodeGenerationContext,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_operand: str = var_name_expressions[0].as_input()
        right_operand = var_name_expressions[1].as_input()
        return [], ExpressionStr(f"{left_operand} + {right_operand}")
