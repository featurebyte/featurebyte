"""
This module contains binary operation node classes
"""

# DO NOT include "from __future__ import annotations" as it will trigger issue for pydantic model nested definition
from typing import ClassVar, List, Sequence, Tuple

from typing_extensions import Literal

from featurebyte.enum import DBVarType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.base import (
    BaseSeriesOutputWithAScalarParamNode,
    BinaryArithmeticOpNode,
    BinaryOpWithBoolOutputNode,
)
from featurebyte.query_graph.node.metadata.config import (
    OnDemandFunctionCodeGenConfig,
    OnDemandViewCodeGenConfig,
)
from featurebyte.query_graph.node.metadata.operation import OperationStructure
from featurebyte.query_graph.node.metadata.sdk_code import (
    ExpressionStr,
    StatementT,
    VariableNameGenerator,
    VarNameExpressionInfo,
)


class AndNode(BinaryOpWithBoolOutputNode):
    """AndNode class"""

    type: Literal[NodeType.AND] = NodeType.AND

    # AND operation is commutative
    is_commutative: ClassVar[bool] = True

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} & {right_operand}"


class OrNode(BinaryOpWithBoolOutputNode):
    """OrNode class"""

    type: Literal[NodeType.OR] = NodeType.OR

    # OR operation is commutative
    is_commutative: ClassVar[bool] = True

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} | {right_operand}"


class EqualNode(BinaryOpWithBoolOutputNode):
    """EqualNode class"""

    type: Literal[NodeType.EQ] = NodeType.EQ

    # Equality operation is commutative
    is_commutative: ClassVar[bool] = True

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} == {right_operand}"


class NotEqualNode(BinaryOpWithBoolOutputNode):
    """NotEqualNode class"""

    type: Literal[NodeType.NE] = NodeType.NE

    # Equality operation is commutative
    is_commutative: ClassVar[bool] = True

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} != {right_operand}"


class GreaterThanNode(BinaryOpWithBoolOutputNode):
    """GreaterThanNode class"""

    type: Literal[NodeType.GT] = NodeType.GT

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} > {right_operand}"


class GreaterEqualNode(BinaryOpWithBoolOutputNode):
    """GreaterEqualNode class"""

    type: Literal[NodeType.GE] = NodeType.GE

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} >= {right_operand}"


class LessThanNode(BinaryOpWithBoolOutputNode):
    """LessThanNode class"""

    type: Literal[NodeType.LT] = NodeType.LT

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} < {right_operand}"


class LessEqualNode(BinaryOpWithBoolOutputNode):
    """LessEqualNode class"""

    type: Literal[NodeType.LE] = NodeType.LE

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} <= {right_operand}"


class AddNode(BinaryArithmeticOpNode):
    """AddNode class"""

    type: Literal[NodeType.ADD] = NodeType.ADD

    # Addition operation is commutative
    is_commutative: ClassVar[bool] = True

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} + {right_operand}"


class SubtractNode(BinaryArithmeticOpNode):
    """SubtractNode class"""

    type: Literal[NodeType.SUB] = NodeType.SUB

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} - {right_operand}"


class MultiplyNode(BinaryArithmeticOpNode):
    """MultiplyNode class"""

    type: Literal[NodeType.MUL] = NodeType.MUL

    # Multiplication operation is commutative
    is_commutative: ClassVar[bool] = True

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} * {right_operand}"


class DivideNode(BinaryArithmeticOpNode):
    """DivideNode class"""

    type: Literal[NodeType.DIV] = NodeType.DIV

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        _ = inputs
        return DBVarType.FLOAT

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} / {right_operand}"

    def generate_odfv_expression(self, left_operand: str, right_operand: str) -> str:
        return f"np.divide({left_operand}, {right_operand})"

    def generate_udf_expression(self, left_operand: str, right_operand: str) -> str:
        return f"np.divide({left_operand}, {right_operand})"


class ModuloNode(BinaryArithmeticOpNode):
    """ModuloNode class"""

    type: Literal[NodeType.MOD] = NodeType.MOD

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} % {right_operand}"


class PowerNode(BaseSeriesOutputWithAScalarParamNode):
    """PowerNode class"""

    type: Literal[NodeType.POWER] = NodeType.POWER

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.FLOAT

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand}.pow({right_operand})"

    def generate_udf_expression(self, left_operand: str, right_operand: str) -> str:
        return f"np.power({left_operand}, {right_operand})"


class IsInNode(BaseSeriesOutputWithAScalarParamNode):
    """IsInNode class"""

    type: Literal[NodeType.IS_IN] = NodeType.IS_IN

    @property
    def max_input_count(self) -> int:
        return 2

    def _get_required_input_columns(
        self, input_index: int, available_column_names: List[str]
    ) -> Sequence[str]:
        return self._assert_empty_required_input_columns()

    def derive_var_type(self, inputs: List[OperationStructure]) -> DBVarType:
        return DBVarType.BOOL

    def generate_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand}.isin({right_operand})"

    def generate_udf_expression(self, left_operand: str, right_operand: str) -> str:
        return f"{left_operand} in {right_operand}"

    def _derive_on_demand_view_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandViewCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_op: str = input_var_name_expressions[0].as_input()
        if len(node_inputs) == 1:
            stats, out_expr = super()._derive_on_demand_view_code(
                node_inputs, var_name_generator, config
            )
            # cast to boolean
            expr = ExpressionStr(f"{out_expr}.apply(lambda x: np.nan if pd.isna(x) else bool(x))")
            return stats, expr

        # handle case when right_operand is an array feature (constructed from count dictionary feature)
        right_op: str = input_var_name_expressions[1].as_input()
        expr = ExpressionStr(
            f"{left_op}.combine({right_op}, lambda x, y: False if pd.isna(x) or not isinstance(y, list) else x in y)"
        )
        return [], expr

    def _derive_user_defined_function_code(
        self,
        node_inputs: List[VarNameExpressionInfo],
        var_name_generator: VariableNameGenerator,
        config: OnDemandFunctionCodeGenConfig,
    ) -> Tuple[List[StatementT], VarNameExpressionInfo]:
        if len(node_inputs) == 1:
            return super()._derive_user_defined_function_code(
                node_inputs, var_name_generator, config
            )

        # handle case when right_operand is an array feature (constructed from count dictionary feature)
        input_var_name_expressions = self._assert_no_info_dict(node_inputs)
        left_op: str = input_var_name_expressions[0].as_input()
        right_op: str = input_var_name_expressions[1].as_input()
        expr = ExpressionStr(
            f"False if pd.isna({left_op}) or not isinstance({right_op}, list) else {left_op} in {right_op}"
        )
        return [], expr
