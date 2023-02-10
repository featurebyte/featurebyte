"""
This module contains unary operation node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Tuple

from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import BaseSeriesOutputNode
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    StatementT,
    StyleConfig,
    VariableNameGenerator,
    VarNameExpression,
)


class BaseUnaryOpNode(BaseSeriesOutputNode, ABC):
    """BaseUnaryOpNode class"""

    @abstractmethod
    def _generate_expression(self, operand: str) -> str:
        """
        Generate expression for the unary operation

        Parameters
        ----------
        operand: str
            Operand

        Returns
        -------
        str
        """

    def _derive_sdk_codes(
        self,
        input_var_name_expressions: List[VarNameExpression],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        style_config: StyleConfig,
    ) -> Tuple[List[StatementT], VarNameExpression]:
        var_name_expression = input_var_name_expressions[0].as_input()
        expression = ExpressionStr(self._generate_expression(var_name_expression))
        return [], expression


class NotNode(BaseUnaryOpNode):
    """NotNode class"""

    type: Literal[NodeType.NOT] = Field(NodeType.NOT, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def _generate_expression(self, operand: str) -> str:
        return f"~{operand}"


class AbsoluteNode(BaseUnaryOpNode):
    """AbsoluteNode class"""

    type: Literal[NodeType.ABS] = Field(NodeType.ABS, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return inputs[0].series_output_dtype

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.abs()"


class SquareRootNode(BaseUnaryOpNode):
    """SquareRootNode class"""

    type: Literal[NodeType.SQRT] = Field(NodeType.SQRT, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.sqrt()"


class FloorNode(BaseUnaryOpNode):
    """FloorNode class"""

    type: Literal[NodeType.FLOOR] = Field(NodeType.FLOOR, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.floor()"


class CeilNode(BaseUnaryOpNode):
    """CeilNode class"""

    type: Literal[NodeType.CEIL] = Field(NodeType.CEIL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.ceil()"


class LogNode(BaseUnaryOpNode):
    """LogNode class"""

    type: Literal[NodeType.LOG] = Field(NodeType.LOG, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.log()"


class ExponentialNode(BaseUnaryOpNode):
    """ExponentialNode class"""

    type: Literal[NodeType.EXP] = Field(NodeType.EXP, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.exp()"


class IsNullNode(BaseUnaryOpNode):
    """IsNullNode class"""

    type: Literal[NodeType.IS_NULL] = Field(NodeType.IS_NULL, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.isnull()"


class CastNode(BaseUnaryOpNode):
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

    def _generate_expression(self, operand: str) -> str:
        return f"{operand}.astype({self.parameters.type})"


class IsStringNode(BaseSeriesOutputNode):
    """IsStringNode class"""

    type: Literal[NodeType.IS_STRING] = Field(NodeType.IS_STRING, const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def _derive_sdk_codes(
        self,
        input_var_name_expressions: List[VarNameExpression],
        input_node_types: List[NodeType],
        var_name_generator: VariableNameGenerator,
        operation_structure: OperationStructure,
        style_config: StyleConfig,
    ) -> Tuple[List[StatementT], VarNameExpression]:
        raise NotImplementedError
