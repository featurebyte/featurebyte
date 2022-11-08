"""
This module contains binary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal

from pydantic import Field

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputWithAScalarParamNode


class AndNode(BaseSeriesOutputWithAScalarParamNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = Field(NodeType.AND, const=True)


class OrNode(BaseSeriesOutputWithAScalarParamNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = Field(NodeType.OR, const=True)


class EqualNode(BaseSeriesOutputWithAScalarParamNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = Field(NodeType.EQ, const=True)


class NotEqualNode(BaseSeriesOutputWithAScalarParamNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = Field(NodeType.NE, const=True)


class GreaterThanNode(BaseSeriesOutputWithAScalarParamNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = Field(NodeType.GT, const=True)


class GreaterEqualNode(BaseSeriesOutputWithAScalarParamNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = Field(NodeType.GE, const=True)


class LessThanNode(BaseSeriesOutputWithAScalarParamNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = Field(NodeType.LT, const=True)


class LessEqualNode(BaseSeriesOutputWithAScalarParamNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = Field(NodeType.LE, const=True)


class AddNode(BaseSeriesOutputWithAScalarParamNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = Field(NodeType.ADD, const=True)


class SubtractNode(BaseSeriesOutputWithAScalarParamNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = Field(NodeType.SUB, const=True)


class MultiplyNode(BaseSeriesOutputWithAScalarParamNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = Field(NodeType.MUL, const=True)


class DivideNode(BaseSeriesOutputWithAScalarParamNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = Field(NodeType.DIV, const=True)


class ModuloNode(BaseSeriesOutputWithAScalarParamNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = Field(NodeType.MOD, const=True)


class PowerNode(BaseSeriesOutputWithAScalarParamNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = Field(NodeType.POWER, const=True)


class ConditionalNode(BaseSeriesOutputWithAScalarParamNode):
    """ConditionalNode class"""

    type: Literal[NodeType.CONDITIONAL] = Field(NodeType.CONDITIONAL, const=True)
