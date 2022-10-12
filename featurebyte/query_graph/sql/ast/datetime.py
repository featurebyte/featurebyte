"""
Module for datetime operations related sql generation
"""
from __future__ import annotations

from typing import Union, cast

from dataclasses import dataclass

import pandas as pd
from sqlglot import Expression, expressions

from featurebyte.common.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext, make_literal_value
from featurebyte.query_graph.sql.ast.generic import ParsedExpressionNode, resolve_project_node
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes


@dataclass
class DatetimeExtractNode(ExpressionNode):
    """Node for extract datetime properties operation"""

    expr: ExpressionNode
    dt_property: DatetimeSupportedPropertyType
    query_node_type = NodeType.DT_EXTRACT

    @property
    def sql(self) -> Expression:
        params = {"this": self.dt_property, "expression": self.expr.sql}
        prop_expr = expressions.Extract(**params)
        if self.dt_property == "dayofweek":
            # pandas: Monday=0, Sunday=6; snowflake: Sunday=0, Saturday=6
            # to follow pandas behavior, add 6 then modulo 7 to perform left-shift
            return expressions.Mod(
                this=expressions.Paren(
                    this=expressions.Add(this=prop_expr, expression=make_literal_value(6))
                ),
                expression=make_literal_value(7),
            )
        return prop_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> DatetimeExtractNode:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        sql_node = DatetimeExtractNode(
            table_node=table_node,
            expr=input_expr_node,
            dt_property=context.parameters["property"],
        )
        return sql_node


@dataclass
class DateDiffNode(ExpressionNode):
    """Node for date difference operation"""

    left_node: ExpressionNode
    right_node: ExpressionNode
    query_node_type = NodeType.DATE_DIFF

    def with_unit(self, unit: TimedeltaSupportedUnitType) -> Expression:
        """Construct a date difference expression with provided time unit

        Parameters
        ----------
        unit : TimedeltaSupportedUnitType
            Time unit

        Returns
        -------
        Expression
        """
        output_expr = expressions.Anonymous(
            this="DATEDIFF",
            expressions=[
                expressions.Identifier(this=unit),
                self.right_node.sql,
                self.left_node.sql,
            ],
        )
        return output_expr

    @property
    def sql(self) -> Expression:
        return self.with_unit("second")

    @classmethod
    def build(cls, context: SQLNodeContext) -> DateDiffNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(
            context.input_sql_nodes, context.parameters
        )
        node = cls(
            table_node=table_node,
            left_node=left_node,
            right_node=right_node,
        )
        return node


@dataclass
class TimedeltaExtractNode(ExpressionNode):
    """Node for converting Timedelta to numeric value given a unit"""

    timedelta_node: Union[TimedeltaNode, DateDiffNode]
    unit: TimedeltaSupportedUnitType
    query_node_type = NodeType.TIMEDELTA_EXTRACT

    @property
    def sql(self) -> Expression:
        if isinstance(self.timedelta_node, DateDiffNode):
            expr = self.timedelta_node.with_unit("microsecond")
            output_expr = self.convert_timedelta_unit(expr, "microsecond", self.unit)
        else:
            output_expr = self.convert_timedelta_unit(
                self.timedelta_node.sql, self.timedelta_node.unit, self.unit
            )
        return output_expr

    @classmethod
    def convert_timedelta_unit(
        cls,
        input_expr: Expression,
        input_unit: TimedeltaSupportedUnitType,
        output_unit: TimedeltaSupportedUnitType,
    ) -> Expression:
        """Create an expression that converts a timedelta column to another unit

        Parameters
        ----------
        input_expr : Expression
            Expression for the timedelta value. Should evaluate to numeric result
        input_unit : TimedeltaSupportedUnitType
            The time unit that input_expr is in
        output_unit : TimedeltaSupportedUnitType
            The desired unit to convert to

        Returns
        -------
        Expression
        """
        input_unit_milli_seconds = int(pd.Timedelta(1, unit=input_unit).total_seconds() * 1e6)
        output_unit_milli_seconds = int(pd.Timedelta(1, unit=output_unit).total_seconds() * 1e6)
        converted_expr = expressions.Div(
            this=expressions.Mul(
                this=input_expr, expression=make_literal_value(input_unit_milli_seconds)
            ),
            expression=make_literal_value(output_unit_milli_seconds),
        )
        converted_expr = expressions.Paren(this=converted_expr)
        return converted_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> TimedeltaExtractNode:
        # Need to retrieve the original DateDiffNode to rewrite the expression with new unit
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        resolved_expr_node = resolve_project_node(input_expr_node)
        assert isinstance(resolved_expr_node, (DateDiffNode, TimedeltaNode))
        sql_node = TimedeltaExtractNode(
            table_node=input_expr_node.table_node,
            timedelta_node=resolved_expr_node,
            unit=context.parameters["property"],
        )
        return sql_node


@dataclass
class TimedeltaNode(ExpressionNode):
    """Node to represent Timedelta"""

    value_expr: ExpressionNode
    unit: TimedeltaSupportedUnitType
    query_node_type = NodeType.TIMEDELTA

    @property
    def sql(self) -> Expression:
        return self.value_expr.sql

    @classmethod
    def build(cls, context: SQLNodeContext) -> TimedeltaNode:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        sql_node = TimedeltaNode(
            table_node=table_node, value_expr=input_expr_node, unit=context.parameters["unit"]
        )
        return sql_node


@dataclass
class DateAddNode(ExpressionNode):
    """Node for date increment by timedelta operation"""

    input_date_node: ExpressionNode
    timedelta_node: Union[TimedeltaNode, DateDiffNode, ParsedExpressionNode]
    query_node_type = NodeType.DATE_ADD

    @property
    def sql(self) -> Expression:
        if isinstance(self.timedelta_node, TimedeltaNode):
            # timedelta is constructed from to_timedelta()
            date_add_args = [
                self.timedelta_node.unit,
                self.timedelta_node.sql,
                self.input_date_node.sql,
            ]
        elif isinstance(self.timedelta_node, DateDiffNode):
            # timedelta is the result of date difference
            date_add_args = [
                "microsecond",
                self.timedelta_node.with_unit("microsecond"),
                self.input_date_node.sql,
            ]
        else:
            # timedelta is a constant value
            date_add_args = [
                "second",
                self.timedelta_node.sql,
                self.input_date_node.sql,
            ]
        output_expr = expressions.Anonymous(this="DATEADD", expressions=date_add_args)
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> DateAddNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(
            context.input_sql_nodes, context.parameters
        )
        resolved_timedelta_node = resolve_project_node(right_node)
        assert isinstance(
            resolved_timedelta_node, (TimedeltaNode, DateDiffNode, ParsedExpressionNode)
        )
        output_node = DateAddNode(
            table_node=table_node,
            input_date_node=left_node,
            timedelta_node=resolved_timedelta_node,
        )
        return output_node
