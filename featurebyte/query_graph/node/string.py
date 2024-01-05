"""
This module contains string operation related node classes
"""
# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import List, Literal, Optional, Tuple

import textwrap
from abc import ABC

from pydantic import BaseModel, Field

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithSingleOperandNode,
    BinaryArithmeticOpNode,
    ValueWithRightOpNodeParameters,
)
from featurebyte.query_graph.node.metadata.config import OnDemandFunctionCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    StatementStr,
    StatementT,
    ValueStr,
    VariableNameGenerator,
    VarNameExpressionInfo,
)

Side = Literal["left", "right", "both"]
Case = Literal["upper", "lower"]


class BaseStringAccessorOpNode(BaseSeriesOutputWithSingleOperandNode, ABC):
    """BaseStringAccessorOpNode class"""

    def _generate_odfv_expression_with_null_value_handling(self, operand: str) -> str:
        # to simulate SQL behavior, we need to handle null value by make it null after operation
        expr = self.generate_odfv_expression(operand)
        where_expr = f"np.where(pd.isna({operand}), np.nan, {expr})"
        return f"pd.Series({where_expr}, index={operand}.index)"


class LengthNode(BaseStringAccessorOpNode):
    """LengthNode class"""

    type: Literal[NodeType.LENGTH] = Field(NodeType.LENGTH, const=True)
    parameters: BaseModel = Field(default=BaseModel(), const=True)

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.INT

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.str.len()"

    def generate_udf_expression(self, operand: str) -> str:
        return f"len({operand})"


class TrimNode(BaseStringAccessorOpNode):
    """TrimNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        character: Optional[str]
        side: Side

    type: Literal[NodeType.TRIM] = Field(NodeType.TRIM, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        value = None
        if self.parameters.character:
            value = ValueStr.create(self.parameters.character)
        if self.parameters.side == "both":
            return f"{operand}.str.strip(to_strip={value})"
        if self.parameters.side == "left":
            return f"{operand}.str.lstrip(to_strip={value})"
        return f"{operand}.str.rstrip(to_strip={value})"

    def generate_udf_expression(self, operand: str) -> str:
        value = None
        if self.parameters.character:
            value = ValueStr.create(self.parameters.character)
        if self.parameters.side == "both":
            return f"{operand}.strip({value})"
        if self.parameters.side == "left":
            return f"{operand}.lstrip({value})"
        return f"{operand}.rstrip({value})"


class ReplaceNode(BaseStringAccessorOpNode):
    """ReplaceNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        replacement: str

    type: Literal[NodeType.REPLACE] = Field(NodeType.REPLACE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        replacement = ValueStr.create(self.parameters.replacement)
        return f"{operand}.str.replace(pat={pattern}, repl={replacement})"

    def generate_udf_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        replacement = ValueStr.create(self.parameters.replacement)
        return f"{operand}.replace({pattern}, {replacement})"


class PadNode(BaseStringAccessorOpNode):
    """PadNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        side: Side
        length: int
        pad: str

    type: Literal[NodeType.PAD] = Field(NodeType.PAD, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        width = self.parameters.length
        side = ValueStr.create(self.parameters.side)
        fill_char = ValueStr.create(self.parameters.pad)
        return f"{operand}.str.pad(width={width}, side={side}, fillchar={fill_char})"

    @staticmethod
    def _get_pad_string_function_name(
        var_name_generator: VariableNameGenerator,
    ) -> Tuple[List[StatementT], str]:
        statements: List[StatementT] = []
        func_name = "pad_string"
        if var_name_generator.should_insert_function(function_name=func_name):
            func_string = """
            def pad_string(input_string: str, side: str, length: int, pad: str) -> str:
                pad_length = length - len(input_string)
                if pad_length <= 0:
                    return input_string  # No padding needed
                if side == "left":
                    return pad * pad_length + input_string
                elif side == "right":
                    return input_string + pad * pad_length
                else:
                    left_pad = (pad_length // 2) * pad
                    right_pad = (pad_length - len(left_pad)) * pad
                    return left_pad + input_string + right_pad
            """
            statements.append(StatementStr(textwrap.dedent(func_string)))
        return statements, func_name

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        statements, func_name = self._get_pad_string_function_name(
            var_name_generator=var_name_generator
        )
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        input_string = input_var_name_expressions[0].as_input()
        side = ValueStr.create(self.parameters.side)
        length = self.parameters.length
        pad = ValueStr.create(self.parameters.pad)
        expr = f"{func_name}(input_string={input_string}, side={side}, length={length}, pad={pad})"
        return statements, ExpressionStr(f"np.nan if pd.isna({input_string}) else {expr}")


class StringCaseNode(BaseStringAccessorOpNode):
    """StringCaseNode class"""

    class Parameters(BaseModel):
        """Parameters class"""

        case: Case

    type: Literal[NodeType.STR_CASE] = Field(NodeType.STR_CASE, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.str.{self.parameters.case}()"

    def generate_udf_expression(self, operand: str) -> str:
        if self.parameters.case == "upper":
            return f"{operand}.upper()"
        return f"{operand}.lower()"


class StringContainsNode(BaseStringAccessorOpNode):
    """StringContainsNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        pattern: str
        case: bool

    type: Literal[NodeType.STR_CONTAINS] = Field(NodeType.STR_CONTAINS, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        return f"{operand}.str.contains(pat={pattern}, case={self.parameters.case})"

    def generate_udf_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        if self.parameters.case:
            return f"{pattern} in {operand}"
        return f"{pattern}.lower() in {operand}.lower()"


class SubStringNode(BaseStringAccessorOpNode):
    """SubStringNode class"""

    class Parameters(BaseModel):
        """Parameters"""

        start: Optional[int]
        length: Optional[int] = Field(default=1, ge=1)

    type: Literal[NodeType.SUBSTRING] = Field(NodeType.SUBSTRING, const=True)
    parameters: Parameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, operand: str) -> str:
        stop = None
        if self.parameters.length is not None:
            stop = self.parameters.length + (self.parameters.start or 0)
        return f"{operand}.str.slice(start={self.parameters.start}, stop={stop})"

    def generate_udf_expression(self, operand: str) -> str:
        stop = None
        if self.parameters.length is not None:
            stop = self.parameters.length + (self.parameters.start or 0)
        return f"{operand}[{self.parameters.start}:{stop}]"


class ConcatNode(BinaryArithmeticOpNode):
    """ConcatNode class"""

    type: Literal[NodeType.CONCAT] = Field(NodeType.CONCAT, const=True)
    parameters: ValueWithRightOpNodeParameters

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.VARCHAR

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"
