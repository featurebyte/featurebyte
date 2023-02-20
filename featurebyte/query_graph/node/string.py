"""
This module contains string operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithSingleOperandNode,
    BinaryArithmeticOpNode,
    ValueWithRightOpNodeParameters,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import ValueStr

Side = Literal["left", "right", "both"]
Case = Literal["upper", "lower"]


class LengthNode(BaseSeriesOutputWithSingleOperandNode):
    """LengthNode class"""

    type: Literal[NodeType.LENGTH] = Field(NodeType.LENGTH, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.str.len()"


class TrimNode(BaseSeriesOutputWithSingleOperandNode):
    """TrimNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        character: Optional[str]
        side: Side

    type: Literal[NodeType.TRIM] = Field(NodeType.TRIM, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        value = None
        if self.parameters.character:
            value = ValueStr.create(self.parameters.character)
        return f"{operand}.str.strip(to_strip={value})"


class ReplaceNode(BaseSeriesOutputWithSingleOperandNode):
    """ReplaceNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        replacement: str

    type: Literal[NodeType.REPLACE] = Field(NodeType.REPLACE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        replacement = ValueStr.create(self.parameters.replacement)
        return f"{operand}.str.replace(pat={pattern}, repl={replacement})"


class PadNode(BaseSeriesOutputWithSingleOperandNode):
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

    def generate_expression(self, operand: str) -> str:
        width = self.parameters.length
        side = ValueStr.create(self.parameters.side)
        fill_char = ValueStr.create(self.parameters.pad)
        return f"{operand}.str.pad(width={width}, side={side}, fillchar={fill_char})"


class StringCaseNode(BaseSeriesOutputWithSingleOperandNode):
    """StringCaseNode class"""

    class Parameters(BaseModel):
        """Parameters class"""

        case: Case

    type: Literal[NodeType.STR_CASE] = Field(NodeType.STR_CASE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.str.{self.parameters.case}()"


class StringContainsNode(BaseSeriesOutputWithSingleOperandNode):
    """StringContainsNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        case: bool

    type: Literal[NodeType.STR_CONTAINS] = Field(NodeType.STR_CONTAINS, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        return f"{operand}.str.contains(pat={pattern}, case={self.parameters.case})"


class SubStringNode(BaseSeriesOutputWithSingleOperandNode):
    """SubStringNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        start: Optional[int]
        length: Optional[int] = Field(default=1, ge=1)

    type: Literal[NodeType.SUBSTRING] = Field(NodeType.SUBSTRING, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        stop = None
        if self.parameters.length is not None:
            stop = self.parameters.length + (self.parameters.start or 0)
        return f"{operand}.str.slice(start={self.parameters.start}, stop={stop})"


class ConcatNode(BinaryArithmeticOpNode):
    """ConcatNode class"""

    type: Literal[NodeType.CONCAT] = Field(NodeType.CONCAT, const=True)
    parameters: ValueWithRightOpNodeParameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"
