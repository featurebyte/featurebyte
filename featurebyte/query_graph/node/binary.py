"""
This module contains binary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal

from pydantic import Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithAScalarParamNode,
    BinaryArithmeticOpMixin,
    BinaryLogicalOpMixin,
    BinaryRelationalOpMixin,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure


class AndNode(BinaryLogicalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = Field(NodeType.AND, const=True)


class OrNode(BinaryLogicalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = Field(NodeType.OR, const=True)


class EqualNode(BinaryRelationalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = Field(NodeType.EQ, const=True)


class NotEqualNode(BinaryRelationalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = Field(NodeType.NE, const=True)


class GreaterThanNode(BinaryRelationalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = Field(NodeType.GT, const=True)


class GreaterEqualNode(BinaryRelationalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = Field(NodeType.GE, const=True)


class LessThanNode(BinaryRelationalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = Field(NodeType.LT, const=True)


class LessEqualNode(BinaryRelationalOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = Field(NodeType.LE, const=True)


class AddNode(BinaryArithmeticOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = Field(NodeType.ADD, const=True)


class SubtractNode(BinaryArithmeticOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = Field(NodeType.SUB, const=True)


class MultiplyNode(BinaryArithmeticOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = Field(NodeType.MUL, const=True)


class DivideNode(BinaryArithmeticOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = Field(NodeType.DIV, const=True)


class ModuloNode(BinaryArithmeticOpMixin, BaseSeriesOutputWithAScalarParamNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = Field(NodeType.MOD, const=True)


class PowerNode(BaseSeriesOutputWithAScalarParamNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = Field(NodeType.POWER, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT


class ConditionalNode(BaseSeriesOutputWithAScalarParamNode):
    """ConditionalNode class"""

    type: Literal[NodeType.CONDITIONAL] = Field(NodeType.CONDITIONAL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype
