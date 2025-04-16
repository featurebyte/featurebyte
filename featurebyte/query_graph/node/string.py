"""
This module contains string operation related node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
import textwrap
from abc import ABC
from typing import List, Optional, Tuple

from pydantic import Field
from typing_extensions import Literal

from featurebyte.enum import DBVarType
from featurebyte.models.base import FeatureByteBaseModel
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model.dtype import DBVarTypeInfo
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithSingleOperandNode,
    BinaryArithmeticOpNode,
    ValueWithRightOpNodeParameters,
)
from featurebyte.query_graph.node.metadata.config import OnDemandFunctionCodeGenConfig
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    NodeCodeGenOutput,
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

    type: Literal[NodeType.LENGTH] = NodeType.LENGTH
    parameters: FeatureByteBaseModel = Field(default_factory=FeatureByteBaseModel)

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.INT)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.str.len()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"{operand}.astype(object).str.len().astype(float)"

    def generate_udf_expression(self, operand: str) -> str:
        return f"len({operand})"


class TrimNode(BaseStringAccessorOpNode):
    """TrimNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        character: Optional[str] = Field(default=None)
        side: Side

    type: Literal[NodeType.TRIM] = NodeType.TRIM
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.VARCHAR)

    def generate_expression(self, operand: str) -> str:
        value = None
        if self.parameters.character:
            value = ValueStr.create(self.parameters.character)
        if self.parameters.side == "both":
            return f"{operand}.str.strip(to_strip={value})"
        if self.parameters.side == "left":
            return f"{operand}.str.lstrip(to_strip={value})"
        return f"{operand}.str.rstrip(to_strip={value})"

    def generate_odfv_expression(self, operand: str) -> str:
        value = None
        if self.parameters.character:
            value = ValueStr.create(self.parameters.character)
        if self.parameters.side == "both":
            return f"{operand}.astype(object).str.strip(to_strip={value})"
        if self.parameters.side == "left":
            return f"{operand}.astype(object).str.lstrip(to_strip={value})"
        return f"{operand}.astype(object).str.rstrip(to_strip={value})"

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

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        pattern: str
        replacement: str

    type: Literal[NodeType.REPLACE] = NodeType.REPLACE
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.VARCHAR)

    def generate_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        replacement = ValueStr.create(self.parameters.replacement)
        return f"{operand}.str.replace(pat={pattern}, repl={replacement})"

    def generate_odfv_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        replacement = ValueStr.create(self.parameters.replacement)
        return f"{operand}.astype(object).str.replace(pat={pattern}, repl={replacement})"

    def generate_udf_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        replacement = ValueStr.create(self.parameters.replacement)
        return f"{operand}.replace({pattern}, {replacement})"


class PadNode(BaseStringAccessorOpNode):
    """PadNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        side: Side
        length: int
        pad: str

    type: Literal[NodeType.PAD] = NodeType.PAD
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.VARCHAR)

    def generate_expression(self, operand: str) -> str:
        width = self.parameters.length
        side = ValueStr.create(self.parameters.side)
        fill_char = ValueStr.create(self.parameters.pad)
        return f"{operand}.str.pad(width={width}, side={side}, fillchar={fill_char})"

    def generate_odfv_expression(self, operand: str) -> str:
        width = self.parameters.length
        side = ValueStr.create(self.parameters.side)
        fill_char = ValueStr.create(self.parameters.pad)
        return f"{operand}.astype(object).str.pad(width={width}, side={side}, fillchar={fill_char})"

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
        node_inputs: List[NodeCodeGenOutput],
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

    class Parameters(FeatureByteBaseModel):
        """Parameters class"""

        case: Case

    type: Literal[NodeType.STR_CASE] = NodeType.STR_CASE
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.VARCHAR)

    def generate_expression(self, operand: str) -> str:
        return f"{operand}.str.{self.parameters.case}()"

    def generate_odfv_expression(self, operand: str) -> str:
        return f"{operand}.astype(object).str.{self.parameters.case}()"

    def generate_udf_expression(self, operand: str) -> str:
        if self.parameters.case == "upper":
            return f"{operand}.upper()"
        return f"{operand}.lower()"


class StringContainsNode(BaseStringAccessorOpNode):
    """StringContainsNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        pattern: str
        case: bool

    type: Literal[NodeType.STR_CONTAINS] = NodeType.STR_CONTAINS
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.BOOL)

    def generate_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        return f"{operand}.str.contains(pat={pattern}, case={self.parameters.case})"

    def generate_odfv_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        return f"{operand}.astype(object).str.contains(pat={pattern}, case={self.parameters.case})"

    def generate_udf_expression(self, operand: str) -> str:
        pattern = ValueStr.create(self.parameters.pattern)
        if self.parameters.case:
            return f"{pattern} in {operand}"
        return f"{pattern}.lower() in {operand}.lower()"


class SubStringNode(BaseStringAccessorOpNode):
    """SubStringNode class"""

    class Parameters(FeatureByteBaseModel):
        """Parameters"""

        start: Optional[int] = Field(default=None)
        length: Optional[int] = Field(default=1, ge=1)

    type: Literal[NodeType.SUBSTRING] = NodeType.SUBSTRING
    parameters: Parameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.VARCHAR)

    def generate_expression(self, operand: str) -> str:
        stop = None
        if self.parameters.length is not None:
            stop = self.parameters.length + (self.parameters.start or 0)
        return f"{operand}.str.slice(start={self.parameters.start}, stop={stop})"

    def generate_odfv_expression(self, operand: str) -> str:
        stop = None
        if self.parameters.length is not None:
            stop = self.parameters.length + (self.parameters.start or 0)
        return f"{operand}.astype(object).str.slice(start={self.parameters.start}, stop={stop})"

    def generate_udf_expression(self, operand: str) -> str:
        stop = None
        if self.parameters.length is not None:
            stop = self.parameters.length + (self.parameters.start or 0)
        return f"{operand}[{self.parameters.start}:{stop}]"


class ConcatNode(BinaryArithmeticOpNode):
    """ConcatNode class"""

    type: Literal[NodeType.CONCAT] = NodeType.CONCAT
    parameters: ValueWithRightOpNodeParameters

    def derive_dtype_info(self, inputs: List[OperationStructure]) -> DBVarTypeInfo:
        return DBVarTypeInfo(dtype=DBVarType.VARCHAR)

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"

    def generate_odfv_expression(self, left_operand: str, right_operand: str) -> str:
        if self.parameters.value is not None:
            return f"{left_operand}.astype(object) + {right_operand}"
        return f"{left_operand}.astype(object) + {right_operand}.astype(object)"

    def generate_udf_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"
