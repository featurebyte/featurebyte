"""
This module contains string operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Tuple

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithAScalarParamNode,
    ValueWithRightOpNodeParameters,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    LiteralStr,
    StatementT,
    StyleConfig,
    VariableNameGenerator,
    VarNameExpression,
)

Side = Literal["left", "right", "both"]
Case = Literal["upper", "lower"]


class BaseStringOpNode(BaseSeriesOutputNode, ABC):
    """BaseStringOpNode class"""

    @abstractmethod
    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
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
        other_operands = [val.as_input() for val in input_var_name_expressions[1:]]
        expression = ExpressionStr(
            self._generate_expression(operand=var_name_expression, other_operands=other_operands)
        )
        return [], expression


class LengthNode(BaseStringOpNode):
    """LengthNode class"""

    type: Literal[NodeType.LENGTH] = Field(NodeType.LENGTH, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        return f"{operand}.str.len()"


class TrimNode(BaseStringOpNode):
    """TrimNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        character: Optional[str]
        side: Side

    type: Literal[NodeType.TRIM] = Field(NodeType.TRIM, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        value = LiteralStr.create(self.parameters.character)
        return f"{operand}.str.strip(to_strip={value})"


class ReplaceNode(BaseStringOpNode):
    """ReplaceNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        replacement: str

    type: Literal[NodeType.REPLACE] = Field(NodeType.REPLACE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        pattern = LiteralStr.create(self.parameters.pattern)
        replacement = LiteralStr.create(self.parameters.replacement)
        return f"{operand}.str.replace(pat={pattern}, repl={replacement})"


class PadNode(BaseStringOpNode):
    """PadNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        side: Side
        length: int
        pad: str

    type: Literal[NodeType.PAD] = Field(NodeType.PAD, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        width = self.parameters.side
        side = LiteralStr.create(self.parameters.side)
        fill_char = LiteralStr.create(self.parameters.pad)
        return f"{operand}.str.pad(width={width}, side={side}, fillchar={fill_char})"


class StringCaseNode(BaseStringOpNode):
    """StringCaseNode class"""

    class Parameters(BaseModel):
        """Parameters class"""

        case: Case

    type: Literal[NodeType.STR_CASE] = Field(NodeType.STR_CASE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        return f"{operand}.str.{self.parameters.case}()"


class StringContainsNode(BaseStringOpNode):
    """StringContainsNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        case: bool

    type: Literal[NodeType.STR_CONTAINS] = Field(NodeType.STR_CONTAINS, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        pattern = LiteralStr.create(self.parameters.pattern)
        return f"{operand}.str.contains(pat={pattern}, case={self.parameters.case})"


class SubStringNode(BaseStringOpNode):
    """SubStringNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        start: Optional[int]
        length: Optional[int] = Field(default=1, ge=1)

    type: Literal[NodeType.SUBSTRING] = Field(NodeType.SUBSTRING, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def _generate_expression(self, operand: str, other_operands: List[str]) -> str:
        start = self.parameters.start
        length = self.parameters.length
        stop = None
        if length is not None:
            stop = length + start
        return f"{operand}.str.slice(start={start}, stop={stop})"


class ConcatNode(BaseSeriesOutputWithAScalarParamNode):
    """ConcatNode class"""

    type: Literal[NodeType.CONCAT] = Field(NodeType.CONCAT, const=True)
    parameters: ValueWithRightOpNodeParameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def _generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"
