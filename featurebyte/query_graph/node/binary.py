"""
This module contains binary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Sequence, Union

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputNode,
    BaseSeriesOutputWithAScalarParamNode,
    BinaryArithmeticOpNode,
    BinaryLogicalOpNode,
    BinaryRelationalOpNode,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure


class AndNode(BinaryLogicalOpNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = Field(NodeType.AND, const=True)


class OrNode(BinaryLogicalOpNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = Field(NodeType.OR, const=True)


class EqualNode(BinaryRelationalOpNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = Field(NodeType.EQ, const=True)


class NotEqualNode(BinaryRelationalOpNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = Field(NodeType.NE, const=True)


class GreaterThanNode(BinaryRelationalOpNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = Field(NodeType.GT, const=True)


class GreaterEqualNode(BinaryRelationalOpNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = Field(NodeType.GE, const=True)


class LessThanNode(BinaryRelationalOpNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = Field(NodeType.LT, const=True)


class LessEqualNode(BinaryRelationalOpNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = Field(NodeType.LE, const=True)


class AddNode(BinaryArithmeticOpNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = Field(NodeType.ADD, const=True)


class SubtractNode(BinaryArithmeticOpNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = Field(NodeType.SUB, const=True)


class MultiplyNode(BinaryArithmeticOpNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = Field(NodeType.MUL, const=True)


class DivideNode(BinaryArithmeticOpNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = Field(NodeType.DIV, const=True)


class ModuloNode(BinaryArithmeticOpNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = Field(NodeType.MOD, const=True)


class PowerNode(BaseSeriesOutputWithAScalarParamNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = Field(NodeType.POWER, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT


class IsInNode(BaseSeriesOutputNode):
    """IsInNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        value: Optional[Sequence[Union[bool, int, float, str]]]

    type: Literal[NodeType.IS_IN] = Field(NodeType.IS_IN, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL
