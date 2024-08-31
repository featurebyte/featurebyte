"""
Module containing string operations related sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Expression
from typing_extensions import Literal

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.ast.util import prepare_unary_input_nodes


@dataclass
class StringCaseNode(ExpressionNode):
    """Node for UPPER, LOWER operation"""

    expr: ExpressionNode
    case: Literal["upper", "lower"]
    query_node_type = NodeType.STR_CASE

    @property
    def sql(self) -> Expression:
        expression = {"upper": expressions.Upper, "lower": expressions.Lower}[self.case]
        return expression(this=self.expr.sql)

    @classmethod
    def build(cls, context: SQLNodeContext) -> StringCaseNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = StringCaseNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            case=parameters["case"],
        )
        return sql_node


@dataclass
class StringContains(ExpressionNode):
    """Node for CONTAINS operation"""

    expr: ExpressionNode
    pattern: str
    case: bool
    query_node_type = NodeType.STR_CONTAINS

    @property
    def sql(self) -> Expression:
        if self.case:
            return self.context.adapter.str_contains(self.expr.sql, self.pattern)
        return self.context.adapter.str_contains(
            expressions.Lower(this=self.expr.sql),
            self.pattern.lower(),
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> StringContains:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = StringContains(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            pattern=parameters["pattern"],
            case=parameters["case"],
        )
        return sql_node


@dataclass
class TrimNode(ExpressionNode):
    """Node for TRIM, LTRIM, RTRIM operations"""

    expr: ExpressionNode
    character: Optional[str]
    side: Literal["left", "right", "both"]
    query_node_type = NodeType.TRIM

    @property
    def sql(self) -> Expression:
        return self.context.adapter.str_trim(
            expr=self.expr.sql, character=self.character, side=self.side
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> TrimNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = TrimNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            character=parameters["character"],
            side=parameters["side"],
        )
        return sql_node


@dataclass
class ReplaceNode(ExpressionNode):
    """Node for REPLACE operation"""

    expr: ExpressionNode
    pattern: str
    replacement: str
    query_node_type = NodeType.REPLACE

    @property
    def sql(self) -> Expression:
        return fb_expressions.Replace(
            this=self.expr.sql,
            pattern=make_literal_value(self.pattern),
            replacement=make_literal_value(self.replacement),
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> ReplaceNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = ReplaceNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            pattern=parameters["pattern"],
            replacement=parameters["replacement"],
        )
        return sql_node


@dataclass
class PadNode(ExpressionNode):
    """Node for LPAD, RPAD operation"""

    expr: ExpressionNode
    side: Literal["left", "right", "both"]
    length: int
    pad: str
    query_node_type = NodeType.PAD

    @staticmethod
    def _generate_pad_expression(
        str_column_expr: Expression,
        target_length_expr: Expression,
        side: Literal["left", "right"],
        pad_char: str,
    ) -> Expression:
        pad_char_expr = make_literal_value(pad_char)
        if side == "left":
            return fb_expressions.LPad(
                this=str_column_expr,
                length=target_length_expr,
                pad=pad_char_expr,
            )
        return fb_expressions.RPad(
            this=str_column_expr,
            length=target_length_expr,
            pad=pad_char_expr,
        )

    @property
    def sql(self) -> Expression:
        target_length_expr = make_literal_value(self.length)
        char_length_expr = expressions.Length(this=self.expr.sql)
        mask_expr = expressions.GTE(this=char_length_expr, expression=target_length_expr)
        if self.side in {"left", "right"}:
            pad_expr = self._generate_pad_expression(
                str_column_expr=self.expr.sql,
                target_length_expr=make_literal_value(self.length),
                side=self.side,  # type: ignore
                pad_char=self.pad,
            )
        else:
            remain_width = expressions.Paren(
                this=expressions.Sub(this=target_length_expr, expression=char_length_expr)
            )
            left_remain_width = expressions.Ceil(
                this=expressions.Div(this=remain_width, expression=make_literal_value(2))
            )
            left_length = expressions.Cast(
                this=expressions.Sub(this=target_length_expr, expression=left_remain_width),
                to=expressions.DataType.build("INT"),
            )
            pad_expr = self._generate_pad_expression(
                str_column_expr=self._generate_pad_expression(
                    str_column_expr=self.expr.sql,
                    target_length_expr=left_length,
                    side="left",
                    pad_char=self.pad,
                ),
                target_length_expr=target_length_expr,
                side="right",
                pad_char=self.pad,
            )
        return expressions.If(this=mask_expr, true=self.expr.sql, false=pad_expr)

    @classmethod
    def build(cls, context: SQLNodeContext) -> PadNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = PadNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            side=parameters["side"],
            length=parameters["length"],
            pad=parameters["pad"],
        )
        return sql_node


@dataclass
class SubStringNode(ExpressionNode):
    """Node for SUBSTRING operation"""

    expr: ExpressionNode
    start: int
    length: Optional[int]
    query_node_type = NodeType.SUBSTRING

    @property
    def sql(self) -> Expression:
        params = {"this": self.expr.sql, "start": make_literal_value(self.start)}
        if self.length is not None:
            params["length"] = make_literal_value(self.length)
        return expressions.Substring(**params)

    @classmethod
    def build(cls, context: SQLNodeContext) -> SubStringNode:
        table_node, input_expr_node, parameters = prepare_unary_input_nodes(context)
        sql_node = SubStringNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
            start=parameters["start"],
            length=parameters["length"],
        )
        return sql_node


@dataclass
class IsStringNode(ExpressionNode):
    """Node for IS_STRING operation to construct a boolean flag to indicate whether the value is string or not."""

    expr: ExpressionNode
    query_node_type = NodeType.IS_STRING

    @property
    def sql(self) -> Expression:
        return self.context.adapter.is_string_type(column_expr=self.expr.sql)

    @classmethod
    def build(cls, context: SQLNodeContext) -> IsStringNode:
        table_node, input_expr_node, _ = prepare_unary_input_nodes(context)
        sql_node = IsStringNode(
            context=context,
            table_node=table_node,
            expr=input_expr_node,
        )
        return sql_node
