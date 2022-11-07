"""
This module contains binary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal

from pydantic import Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputWithScalarInputSeriesOutputNode


class AndWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = Field(NodeType.AND, const=True)


class OrWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = Field(NodeType.OR, const=True)


class EqualWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = Field(NodeType.EQ, const=True)


class NotEqualWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = Field(NodeType.NE, const=True)


class GreaterThanWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = Field(NodeType.GT, const=True)


class GreaterEqualWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = Field(NodeType.GE, const=True)


class LessThanWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = Field(NodeType.LT, const=True)


class LessEqualWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = Field(NodeType.LE, const=True)


class AddWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = Field(NodeType.ADD, const=True)


class SubtractWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = Field(NodeType.SUB, const=True)


class MultiplyWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = Field(NodeType.MUL, const=True)


class DivideWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = Field(NodeType.DIV, const=True)


class ModuloWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = Field(NodeType.MOD, const=True)


class PowerWithScalarInputNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = Field(NodeType.POWER, const=True)


class ConditionalNode(BaseSeriesOutputWithScalarInputSeriesOutputNode):
    """ConditionalNode class"""

    type: Literal[NodeType.CONDITIONAL] = Field(NodeType.CONDITIONAL, const=True)
