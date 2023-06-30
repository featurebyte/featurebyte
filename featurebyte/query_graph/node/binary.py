"""
This module contains binary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Sequence

from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithAScalarParamNode,
    BinaryArithmeticOpNode,
    BinaryLogicalOpNode,
    BinaryRelationalOpNode,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure


class AndNode(BinaryLogicalOpNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = Field(NodeType.AND, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} & {right_operand}"


class OrNode(BinaryLogicalOpNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = Field(NodeType.OR, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} | {right_operand}"


class EqualNode(BinaryRelationalOpNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = Field(NodeType.EQ, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} == {right_operand}"


class NotEqualNode(BinaryRelationalOpNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = Field(NodeType.NE, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} != {right_operand}"


class GreaterThanNode(BinaryRelationalOpNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = Field(NodeType.GT, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} > {right_operand}"


class GreaterEqualNode(BinaryRelationalOpNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = Field(NodeType.GE, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} >= {right_operand}"


class LessThanNode(BinaryRelationalOpNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = Field(NodeType.LT, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} < {right_operand}"


class LessEqualNode(BinaryRelationalOpNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = Field(NodeType.LE, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} <= {right_operand}"


class AddNode(BinaryArithmeticOpNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = Field(NodeType.ADD, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"


class SubtractNode(BinaryArithmeticOpNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = Field(NodeType.SUB, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} - {right_operand}"


class MultiplyNode(BinaryArithmeticOpNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = Field(NodeType.MUL, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} * {right_operand}"


class DivideNode(BinaryArithmeticOpNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = Field(NodeType.DIV, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} / {right_operand}"


class ModuloNode(BinaryArithmeticOpNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = Field(NodeType.MOD, const=True)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} % {right_operand}"


class PowerNode(BaseSeriesOutputWithAScalarParamNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = Field(NodeType.POWER, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand}.pow({right_operand})"


class IsInNode(BaseSeriesOutputWithAScalarParamNode):
    """IsInNode class"""

    type: Literal[NodeType.IS_IN] = Field(NodeType.IS_IN, const=True)

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand}.isin({right_operand})"
