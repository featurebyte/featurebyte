"""
This module contains unary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import ClassVar, List, Literal, Type, Union

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputWithSingleOperandNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import ExpressionStr, VariableNameStr


class NotNode(BaseSeriesOutputWithSingleOperandNode):
    """NotNode class"""

    type: Literal[NodeType.NOT] = Field(NodeType.NOT, const=True)

    # class variable
    _derive_sdk_code_return_var_name_expression_type: ClassVar[
        Union[Type[VariableNameStr], Type[ExpressionStr]]
    ] = ExpressionStr

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, operand: str) -> str:
        return f"~{operand}"


class AbsoluteNode(BaseSeriesOutputWithSingleOperandNode):
    """AbsoluteNode class"""

    type: Literal[NodeType.ABS] = Field(NodeType.ABS, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.abs()"


class SquareRootNode(BaseSeriesOutputWithSingleOperandNode):
    """SquareRootNode class"""

    type: Literal[NodeType.SQRT] = Field(NodeType.SQRT, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.sqrt()"


class FloorNode(BaseSeriesOutputWithSingleOperandNode):
    """FloorNode class"""

    type: Literal[NodeType.FLOOR] = Field(NodeType.FLOOR, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.floor()"


class CeilNode(BaseSeriesOutputWithSingleOperandNode):
    """CeilNode class"""

    type: Literal[NodeType.CEIL] = Field(NodeType.CEIL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.ceil()"


class LogNode(BaseSeriesOutputWithSingleOperandNode):
    """LogNode class"""

    type: Literal[NodeType.LOG] = Field(NodeType.LOG, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.log()"


class ExponentialNode(BaseSeriesOutputWithSingleOperandNode):
    """ExponentialNode class"""

    type: Literal[NodeType.EXP] = Field(NodeType.EXP, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.exp()"


class IsNullNode(BaseSeriesOutputWithSingleOperandNode):
    """IsNullNode class"""

    type: Literal[NodeType.IS_NULL] = Field(NodeType.IS_NULL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.isnull()"


class CastNode(BaseSeriesOutputWithSingleOperandNode):
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

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.astype({self.parameters.type})"


class IsStringNode(BaseSeriesOutputWithSingleOperandNode):
    """IsStringNode class"""

    type: Literal[NodeType.IS_STRING] = Field(NodeType.IS_STRING, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, operand: str) -> str:
        raise RuntimeError("Not implemented")
