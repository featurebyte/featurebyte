"""
This module contains binary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode


class BaseBinaryOpNode(BaseNode):
    """Base class for binary operation node"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)


class BaseBinaryLogicalOpNode(BaseBinaryOpNode):
    """Base class for binary logical operation node"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Any]

    parameters: Parameters


class AndNode(BaseBinaryLogicalOpNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = Field(NodeType.AND, const=True)


class OrNode(BaseBinaryLogicalOpNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = Field(NodeType.OR, const=True)


class BaseBinaryRelationalOpNode(BaseBinaryOpNode):
    """Base class for binary relational operation node"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Union[int, float, str, bool]]

    parameters: Parameters


class EqualNode(BaseBinaryRelationalOpNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = Field(NodeType.EQ, const=True)


class NotEqualNode(BaseBinaryRelationalOpNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = Field(NodeType.NE, const=True)


class GreaterThanNode(BaseBinaryRelationalOpNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = Field(NodeType.GT, const=True)


class GreaterEqualNode(BaseBinaryRelationalOpNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = Field(NodeType.GE, const=True)


class LessThanNode(BaseBinaryRelationalOpNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = Field(NodeType.LT, const=True)


class LessEqualNode(BaseBinaryRelationalOpNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = Field(NodeType.LE, const=True)


class BaseBinaryArithmeticOpNode(BaseBinaryOpNode):
    """Base class for binary arithmetic operation node"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Any]

    parameters: Parameters


class AddNode(BaseBinaryArithmeticOpNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = Field(NodeType.ADD, const=True)


class SubtractNode(BaseBinaryArithmeticOpNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = Field(NodeType.SUB, const=True)


class MultiplyNode(BaseBinaryArithmeticOpNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = Field(NodeType.MUL, const=True)


class DivideNode(BaseBinaryArithmeticOpNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = Field(NodeType.DIV, const=True)


class ModuloNode(BaseBinaryArithmeticOpNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = Field(NodeType.MOD, const=True)


class PowerNode(BaseBinaryArithmeticOpNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = Field(NodeType.POWER, const=True)
