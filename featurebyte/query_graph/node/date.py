"""
This module contains datetime operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Tuple

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode, BinaryArithmeticOpNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    StatementT,
    StyleConfig,
    VariableNameGenerator,
    VarNameExpression,
)


class BaseDateOpNode(BaseSeriesOutputNode, ABC):
    """BaseDateOpNode class"""

    @abstractmethod
    def _generate_expression(self, operand: str) -> str:
        """
        Generate expression for the unary operation

        Parameters
        ----------
        operand: str
            Operand

        Returns
        -------
        str
        """

    def _derive_sdk_codes(
        self,
        input_var_name_expressions: List[VarNameExpression],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        style_config: StyleConfig,
    ) -> Tuple[List[StatementT], VarNameExpression]:
        var_name_expression = input_var_name_expressions[0].as_input()
        expression = ExpressionStr(self._generate_expression(var_name_expression))
        return [], expression


class DatetimeExtractNode(BaseDateOpNode):
    """DatetimeExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: DatetimeSupportedPropertyType

    type: Literal[NodeType.DT_EXTRACT] = Field(NodeType.DT_EXTRACT, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def _generate_expression(self, operand: str) -> str:
        date_property = self.parameters.property
        if date_property == "dayofweek":
            date_property = "day_of_week"
        return f"{operand}.dt.{date_property}"


class TimeDeltaExtractNode(BaseDateOpNode):
    """TimeDeltaExtractNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        property: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA_EXTRACT] = Field(NodeType.TIMEDELTA_EXTRACT, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.dt.{self.parameters.property}"


class DateDifference(BinaryArithmeticOpNode):
    """DateDifference class"""

    type: Literal[NodeType.DATE_DIFF] = Field(NodeType.DATE_DIFF, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.TIMEDELTA

    def _generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} - {right_operand}"


class TimeDelta(BaseDateOpNode):
    """TimeDelta class"""

    class Parameters(BaseModel):
        """Parameters"""

        unit: TimedeltaSupportedUnitType

    type: Literal[NodeType.TIMEDELTA] = Field(NodeType.TIMEDELTA, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.TIMEDELTA

    def _generate_expression(self, operand: str) -> str:
        return f"to_timedelta(series={operand}, unit={self.parameters.unit})"


class DateAdd(BinaryArithmeticOpNode):
    """DateAdd class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[int]

    type: Literal[NodeType.DATE_ADD] = Field(NodeType.DATE_ADD, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].columns[0].dtype

    def _generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"
