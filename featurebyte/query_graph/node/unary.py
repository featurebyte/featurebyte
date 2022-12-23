"""
This module contains unary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Sequence, Union

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure


class NotNode(BaseSeriesOutputNode):
    """NotNode class"""

    type: Literal[NodeType.NOT] = Field(NodeType.NOT, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class AbsoluteNode(BaseSeriesOutputNode):
    """AbsoluteNode class"""

    type: Literal[NodeType.ABS] = Field(NodeType.ABS, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype


class SquareRootNode(BaseSeriesOutputNode):
    """SquareRootNode class"""

    type: Literal[NodeType.SQRT] = Field(NodeType.SQRT, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT


class FloorNode(BaseSeriesOutputNode):
    """FloorNode class"""

    type: Literal[NodeType.FLOOR] = Field(NodeType.FLOOR, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT


class CeilNode(BaseSeriesOutputNode):
    """CeilNode class"""

    type: Literal[NodeType.CEIL] = Field(NodeType.CEIL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT


class LogNode(BaseSeriesOutputNode):
    """LogNode class"""

    type: Literal[NodeType.LOG] = Field(NodeType.LOG, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT


class ExponentialNode(BaseSeriesOutputNode):
    """ExponentialNode class"""

    type: Literal[NodeType.EXP] = Field(NodeType.EXP, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT


class IsNullNode(BaseSeriesOutputNode):
    """IsNullNode class"""

    type: Literal[NodeType.IS_NULL] = Field(NodeType.IS_NULL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class CastNode(BaseSeriesOutputNode):
    """CastNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        type: Literal["int", "float", "str"]
        from_dtype: DBVarType

    type: Literal[NodeType.CAST] = Field(NodeType.CAST, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        if self.parameters.type == "int":
            return DBVarType.INT
        if self.parameters.type == "float":
            return DBVarType.FLOAT
        if self.parameters.type == "str":
            return DBVarType.VARCHAR
        return DBVarType.UNKNOWN  # type: ignore[unreachable]


class IsInNode(BaseSeriesOutputNode):
    """IsInNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        values: Sequence[Union[bool, int, float, str]]

    type: Literal[NodeType.IS_IN] = Field(NodeType.IS_IN, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL


class IsStringNode(BaseSeriesOutputNode):
    """IsStringNode class"""

    type: Literal[NodeType.IS_STRING] = Field(NodeType.IS_STRING, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL
