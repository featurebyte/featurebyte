"""
This module contains unary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import Literal, Optional

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.column import OutColumnStr


class NotNode(BaseSeriesOutputNode):
    """NotNode class"""

    type: Literal[NodeType.NOT] = Field(NodeType.NOT, const=True)


class AbsoluteNode(BaseSeriesOutputNode):
    """AbsoluteNode class"""

    type: Literal[NodeType.ABS] = Field(NodeType.ABS, const=True)


class SquareRootNode(BaseSeriesOutputNode):
    """SquareRootNode class"""

    type: Literal[NodeType.SQRT] = Field(NodeType.SQRT, const=True)


class FloorNode(BaseSeriesOutputNode):
    """FloorNode class"""

    type: Literal[NodeType.FLOOR] = Field(NodeType.FLOOR, const=True)


class CeilNode(BaseSeriesOutputNode):
    """CeilNode class"""

    type: Literal[NodeType.CEIL] = Field(NodeType.CEIL, const=True)


class LogNode(BaseSeriesOutputNode):
    """LogNode class"""

    type: Literal[NodeType.LOG] = Field(NodeType.LOG, const=True)


class ExponentialNode(BaseSeriesOutputNode):
    """ExponentialNode class"""

    type: Literal[NodeType.EXP] = Field(NodeType.EXP, const=True)


class IsNullNode(BaseSeriesOutputNode):
    """IsNullNode class"""

    type: Literal[NodeType.IS_NULL] = Field(NodeType.IS_NULL, const=True)


class CastNode(BaseSeriesOutputNode):
    """CastNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        type: Literal["int", "float", "str"]
        from_dtype: DBVarType

    type: Literal[NodeType.CAST] = Field(NodeType.CAST, const=True)
    parameters: Parameters


class AliasNode(BaseSeriesOutputNode):
    """AliasNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        name: OutColumnStr

    type: Literal[NodeType.ALIAS] = Field(NodeType.ALIAS, const=True)
    parameters: Parameters

    def _get_output_name(self) -> Optional[str]:
        return self.parameters.name
