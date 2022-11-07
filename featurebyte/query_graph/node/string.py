"""
This module contains string operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal, Optional

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithScalarInputSeriesOutputNode,
)

Side = Literal["left", "right", "both"]
Case = Literal["upper", "lower"]


class LengthNode(BaseSeriesOutputNode):
    """LengthNode class"""

    type: Literal[NodeType.LENGTH] = Field(NodeType.LENGTH, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)


class TrimNode(BaseSeriesOutputNode):
    """TrimNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        character: Optional[str]
        side: Side

    type: Literal[NodeType.TRIM] = Field(NodeType.TRIM, const=True)
    parameters: Parameters


class ReplaceNode(BaseSeriesOutputNode):
    """ReplaceNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        replacement: str

    type: Literal[NodeType.REPLACE] = Field(NodeType.REPLACE, const=True)
    parameters: Parameters


class PadNode(BaseSeriesOutputNode):
    """PadNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        side: Side
        length: int
        pad: str

    type: Literal[NodeType.PAD] = Field(NodeType.PAD, const=True)
    parameters: Parameters


class StringCaseNode(BaseSeriesOutputNode):
    """StringCaseNode class"""

    class Parameters(BaseModel):
        """Parameters class"""

        case: Case

    type: Literal[NodeType.STR_CASE] = Field(NodeType.STR_CASE, const=True)
    parameters: Parameters


class StringContainsNode(BaseSeriesOutputNode):
    """StringContainsNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        case: bool

    type: Literal[NodeType.STR_CONTAINS] = Field(NodeType.STR_CONTAINS, const=True)
    parameters: Parameters


class SubStringNode(BaseSeriesOutputNode):
    """SubStringNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        start: Optional[int]
        length: Optional[int] = Field(default=1, ge=1)

    type: Literal[NodeType.SUBSTRING] = Field(NodeType.SUBSTRING, const=True)
    parameters: Parameters


class ConcatWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """ConcatNode class"""

    type: Literal[NodeType.CONCAT] = Field(NodeType.CONCAT, const=True)
