"""
Module containing string operations related sql generation
"""
from typing import Literal, Optional

from dataclasses import dataclass

from sqlglot import Expression, expressions

from featurebyte.query_graph.sql import expression as fb_expressions
from featurebyte.query_graph.sql.ast.base import ExpressionNode, make_literal_value


@dataclass
class StringCaseNode(ExpressionNode):
    """Node for UPPER, LOWER operation"""

    expr: ExpressionNode
    case: Literal["upper", "lower"]

    @property
    def sql(self) -> Expression:
        expression = {"upper": expressions.Upper, "lower": expressions.Lower}[self.case]
        return expression(this=self.expr.sql)


@dataclass
class StringContains(ExpressionNode):
    """Node for CONTAINS operation"""

    expr: ExpressionNode
    pattern: str
    case: bool

    @property
    def sql(self) -> Expression:
        if self.case:
            return fb_expressions.Contains(
                this=self.expr.sql,
                pattern=make_literal_value(self.pattern),
            )
        return fb_expressions.Contains(
            this=expressions.Lower(this=self.expr.sql),
            pattern=expressions.Lower(this=make_literal_value(self.pattern)),
        )


@dataclass
class TrimNode(ExpressionNode):
    """Node for TRIM, LTRIM, RTRIM operations"""

    expr: ExpressionNode
    character: Optional[str]
    side: Literal["left", "right", "both"]

    @property
    def sql(self) -> Expression:
        expression_class = {
            "left": fb_expressions.LTrim,
            "right": fb_expressions.RTrim,
            "both": fb_expressions.Trim,
        }[self.side]
        if self.character:
            return expression_class(
                this=self.expr.sql, character=make_literal_value(self.character)
            )
        return expression_class(this=self.expr.sql)


@dataclass
class ReplaceNode(ExpressionNode):
    """Node for REPLACE operation"""

    expr: ExpressionNode
    pattern: str
    replacement: str

    @property
    def sql(self) -> Expression:
        return fb_expressions.Replace(
            this=self.expr.sql,
            pattern=make_literal_value(self.pattern),
            replacement=make_literal_value(self.replacement),
        )


@dataclass
class PadNode(ExpressionNode):
    """Node for LPAD, RPAD operation"""

    expr: ExpressionNode
    side: Literal["left", "right", "both"]
    length: int
    pad: str

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
            left_length = expressions.Sub(this=target_length_expr, expression=left_remain_width)
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


@dataclass
class SubStringNode(ExpressionNode):
    """Node for SUBSTRING operation"""

    expr: ExpressionNode
    start: int
    length: Optional[int]

    @property
    def sql(self) -> Expression:
        params = {"this": self.expr.sql, "start": make_literal_value(self.start)}
        if self.length is not None:
            params["length"] = make_literal_value(self.length)
        return expressions.Substring(**params)
