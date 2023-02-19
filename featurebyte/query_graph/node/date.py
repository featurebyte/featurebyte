"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import ClassVar, List, Literal, Optional, Tuple, Type, Union

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
    ExpressionStr,
    StatementT,
    VariableNameGenerator,
    VariableNameStr,
    VarNameExpressionStr,
)


class DatetimeExtractNode(BaseSeriesOutputWithSingleOperandNode):
    """DatetimeExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: DatetimeSupportedPropertyType

    type: Literal[NodeType.DT_EXTRACT] = Field(NodeType.DT_EXTRACT, const=True)
    parameters: Parameters

    # class variable
    _derive_sdk_code_return_var_name_expression_type: ClassVar[
        Union[Type[VariableNameStr], Type[ExpressionStr]]
    ] = VariableNameStr

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def _generate_expression(self, operand: str) -> str:
        date_property = self.parameters.property
        if date_property == "dayofweek":
            date_property = "day_of_week"
        return f"{operand}.dt.{date_property}"


class TimeDeltaExtractNode(BaseSeriesOutputWithSingleOperandNode):
    """TimeDeltaExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA_EXTRACT] = Field(NodeType.TIMEDELTA_EXTRACT, const=True)
    parameters: Parameters

    # class variable
    _derive_sdk_code_return_var_name_expression_type: ClassVar[
        Union[Type[VariableNameStr], Type[ExpressionStr]]
    ] = VariableNameStr

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.dt.{self.parameters.property}"


class DateDifference(BaseSeriesOutputNode):
    """DateDifference class"""

    type: Literal[NodeType.DATE_DIFF] = Field(NodeType.DATE_DIFF, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.TIMEDELTA

    def _derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        left_operand: str = input_var_name_expressions[0].as_input()
        right_operand = input_var_name_expressions[1].as_input()
        return [], ExpressionStr(f"{left_operand} - {right_operand}")


class TimeDelta(BaseSeriesOutputNode):
    """TimeDelta class"""

    class Parameters(BaseModel):
        """Parameters"""

        unit: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA] = Field(NodeType.TIMEDELTA, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.TIMEDELTA

    def _derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        var_name_expression = input_var_name_expressions[0]
        statements = []
        var_name = var_name_generator.generate_variable_name(
            node_output_type=operation_structure.output_type,
            node_output_category=operation_structure.output_category,
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

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].columns[0].dtype

    def _derive_sdk_code(
        self,
        input_var_name_expressions: List[VarNameExpressionStr],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        config: CodeGenerationConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionStr]:
        left_operand: str = input_var_name_expressions[0].as_input()
        right_operand = input_var_name_expressions[1].as_input()
        return [], ExpressionStr(f"{left_operand} + {right_operand}")
