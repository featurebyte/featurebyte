"""
This module contains string operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithAScalarParamNode,
    ValueWithRightOpNodeParameters,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure

Side = Literal["left", "right", "both"]
Case = Literal["upper", "lower"]


class LengthNode(BaseSeriesOutputNode):
    """LengthNode class"""

    type: Literal[NodeType.LENGTH] = Field(NodeType.LENGTH, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT


class TrimNode(BaseSeriesOutputNode):
    """TrimNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        character: Optional[str]
        side: Side

    type: Literal[NodeType.TRIM] = Field(NodeType.TRIM, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR


class ReplaceNode(BaseSeriesOutputNode):
    """ReplaceNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        replacement: str

    type: Literal[NodeType.REPLACE] = Field(NodeType.REPLACE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR


class PadNode(BaseSeriesOutputNode):
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


class StringCaseNode(BaseSeriesOutputNode):
    """StringCaseNode class"""

    class Parameters(BaseModel):
        """Parameters class"""

        case: Case

    type: Literal[NodeType.STR_CASE] = Field(NodeType.STR_CASE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR


class StringContainsNode(BaseSeriesOutputNode):
    """StringContainsNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        case: bool

    type: Literal[NodeType.STR_CONTAINS] = Field(NodeType.STR_CONTAINS, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class SubStringNode(BaseSeriesOutputNode):
    """SubStringNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        start: Optional[int]
        length: Optional[int] = Field(default=1, ge=1)

    type: Literal[NodeType.SUBSTRING] = Field(NodeType.SUBSTRING, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR


class ConcatNode(BaseSeriesOutputWithAScalarParamNode):
    """ConcatNode class"""

    type: Literal[NodeType.CONCAT] = Field(NodeType.CONCAT, const=True)
    parameters: ValueWithRightOpNodeParameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR
