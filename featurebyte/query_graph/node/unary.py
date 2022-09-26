"""
This module contains unary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal

from pydantic import BaseModel, Field

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.base import BaseNode


class BaseUnaryOpNode(BaseNode):
    """Base class for unary operation node"""

    output_type: NodeOutputType = Field(NodeOutputType.SERIES, const=True)
    parameters: BaseModel = Field(default_factory=BaseModel, const=True)


class NotNode(BaseUnaryOpNode):
    """NotNode class"""

    type: Literal[NodeType.NOT] = Field(NodeType.NOT, const=True)


class AbsoluteNode(BaseUnaryOpNode):
    """AbsoluteNode class"""

    type: Literal[NodeType.ABS] = Field(NodeType.ABS, const=True)


class SquareRootNode(BaseUnaryOpNode):
    """SquareRootNode class"""

    type: Literal[NodeType.SQRT] = Field(NodeType.SQRT, const=True)


class FloorNode(BaseUnaryOpNode):
    """FloorNode class"""

    type: Literal[NodeType.FLOOR] = Field(NodeType.FLOOR, const=True)


class CeilNode(BaseUnaryOpNode):
    """CeilNode class"""

    type: Literal[NodeType.CEIL] = Field(NodeType.CEIL, const=True)


class LogNode(BaseUnaryOpNode):
    """LogNode class"""

    type: Literal[NodeType.LOG] = Field(NodeType.LOG, const=True)


class ExponentialNode(BaseUnaryOpNode):
    """ExponentialNode class"""

    type: Literal[NodeType.EXP] = Field(NodeType.EXP, const=True)


class IsNullNode(BaseUnaryOpNode):
    """IsNullNode class"""

    type: Literal[NodeType.IS_NULL] = Field(NodeType.IS_NULL, const=True)
