"""
Module for datetime operations related sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Union, cast

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.date import (
    DateDifferenceParameters,
    DatetimeExtractNodeParameters,
)
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.base import ExpressionNode, SQLNodeContext
from featurebyte.query_graph.sql.ast.generic import ParsedExpressionNode, resolve_project_node
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.ast.util import prepare_binary_op_input_nodes
from featurebyte.query_graph.sql.timestamp_helper import (
    convert_timestamp_timezone_tuple,
    convert_timestamp_to_local,
    convert_timestamp_to_utc,
)
from featurebyte.typing import DatetimeSupportedPropertyType, TimedeltaSupportedUnitType


@dataclass
class DatetimeExtractNode(ExpressionNode):
    """Node for extract datetime properties operation"""

    expr: Expression
    dt_property: DatetimeSupportedPropertyType
    query_node_type = NodeType.DT_EXTRACT

    @property
    def sql(self) -> Expression:
        params = {
            "this": expressions.Var(
                this=self.context.adapter.get_datetime_extract_property(self.dt_property)
            ),
            "expression": self.expr,
        }
        prop_expr = expressions.Extract(**params)
        if self.dt_property == "dayofweek":
            return self.context.adapter.adjust_dayofweek(prop_expr)
        if self.dt_property == "second":
            # remove the fraction component
            return expressions.Floor(this=prop_expr)
        return prop_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> DatetimeExtractNode:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        parameters = DatetimeExtractNodeParameters(**context.parameters)

        # Extract timezone offset expression (can be a fixed value or a column)
        if parameters.timezone_offset is not None or len(context.input_sql_nodes) > 1:
            if len(context.input_sql_nodes) > 1:
                offset_expr_node = cast(ExpressionNode, context.input_sql_nodes[1])
            else:
                offset_expr_node = None
            timestamp_expr = cls._get_local_timestamp_expr_event_table(
                parameters, input_expr_node, offset_expr_node, context.adapter
            )
        elif parameters.timestamp_schema is not None:
            timestamp_expr = convert_timestamp_to_local(
                input_expr_node.sql, parameters.timestamp_schema, context.adapter
            )
        elif parameters.timestamp_tuple_schema is not None:
            timestamp_expr = convert_timestamp_timezone_tuple(
                zipped_expr=input_expr_node.sql,
                target_tz="local",
                timestamp_tuple_schema=parameters.timestamp_tuple_schema,
                adapter=context.adapter,
            )
        else:
            timestamp_expr = input_expr_node.sql

        sql_node = DatetimeExtractNode(
            context=context,
            table_node=table_node,
            expr=timestamp_expr,
            dt_property=context.parameters["property"],
        )
        return sql_node

    @classmethod
    def _get_local_timestamp_expr_event_table(
        cls,
        parameters: DatetimeExtractNodeParameters,
        input_expr_node: ExpressionNode,
        offset_expr_node: Optional[ExpressionNode],
        adapter: BaseAdapter,
    ) -> Expression:
        # Extract timezone offset expression (can be a fixed value or a column)
        if parameters.timezone_offset is not None:
            timezone_offset_expr = make_literal_value(parameters.timezone_offset)
        else:
            assert offset_expr_node is not None
            timezone_offset_expr = offset_expr_node.sql

        # If timezone offset is provided, apply that to the input timestamp (in UTC) to obtain
        # the local time before extracting date properties
        timezone_offset_seconds = adapter.call_udf(
            "F_TIMEZONE_OFFSET_TO_SECOND",
            [timezone_offset_expr],
        )
        timestamp_expr = adapter.dateadd_second(timezone_offset_seconds, input_expr_node.sql)

        return timestamp_expr


@dataclass
class DateDiffNode(ExpressionNode):
    """Node for date difference operation"""

    left_node: ExpressionNode
    right_node: ExpressionNode
    query_node_type = NodeType.DATE_DIFF

    def total_microseconds(self) -> Expression:
        """Construct a date difference expression in microseconds

        Returns
        -------
        Expression
        """
        output_expr = self.context.adapter.datediff_microsecond(
            timestamp_expr_1=self.right_node.sql,
            timestamp_expr_2=self.left_node.sql,
        )
        return output_expr

    @property
    def sql(self) -> Expression:
        # The behaviour of DATEDIFF given a time unit depends on the engine; some engines perform
        # rounding but others don't. To keep a consistent behaviour, always work in the highest
        # supported precision (microsecond) and convert the result back to the desired unit
        # explicitly.
        return TimedeltaExtractNode.convert_timedelta_unit(
            input_expr=self.total_microseconds(), input_unit="microsecond", output_unit="second"
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> DateDiffNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        parameters = DateDifferenceParameters(**context.parameters)
        if parameters.left_timestamp_schema:
            left_node = ParsedExpressionNode.from_expression_node(
                left_node,
                convert_timestamp_to_utc(
                    left_node.sql, parameters.left_timestamp_schema, context.adapter
                ),
            )
        elif parameters.left_timestamp_tuple_schema:
            left_node = ParsedExpressionNode.from_expression_node(
                left_node,
                convert_timestamp_timezone_tuple(
                    zipped_expr=left_node.sql,
                    target_tz="utc",
                    timestamp_tuple_schema=parameters.left_timestamp_tuple_schema,
                    adapter=context.adapter,
                ),
            )
        if parameters.right_timestamp_schema:
            right_node = ParsedExpressionNode.from_expression_node(
                right_node,
                convert_timestamp_to_utc(
                    right_node.sql, parameters.right_timestamp_schema, context.adapter
                ),
            )
        elif parameters.right_timestamp_tuple_schema:
            right_node = ParsedExpressionNode.from_expression_node(
                right_node,
                convert_timestamp_timezone_tuple(
                    zipped_expr=right_node.sql,
                    target_tz="utc",
                    timestamp_tuple_schema=parameters.right_timestamp_tuple_schema,
                    adapter=context.adapter,
                ),
            )
        node = cls(
            context=context,
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
            expr = self.timedelta_node.total_microseconds()
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

        def _make_quantity_in_microsecond(unit: TimedeltaSupportedUnitType) -> Expression:
            quantity = int(pd.Timedelta(1, unit=unit).total_seconds() * 1e6)
            quantity_expr = make_literal_value(quantity)
            # cast to LONG type to avoid overflow in some engines (e.g. databricks)
            quantity_expr = expressions.Cast(
                this=quantity_expr, to=expressions.DataType.build("BIGINT")
            )
            return quantity_expr

        input_unit_microsecond = _make_quantity_in_microsecond(input_unit)
        output_unit_microsecond = _make_quantity_in_microsecond(output_unit)
        conversion_factor_expr = expressions.Div(
            this=input_unit_microsecond, expression=output_unit_microsecond
        )
        converted_expr = expressions.Mul(this=input_expr, expression=conversion_factor_expr)  # type: Expression
        converted_expr = expressions.Paren(this=converted_expr)
        return converted_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> TimedeltaExtractNode:
        # Need to retrieve the original DateDiffNode to rewrite the expression with new unit
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        resolved_expr_node = resolve_project_node(input_expr_node)
        assert isinstance(resolved_expr_node, (DateDiffNode, TimedeltaNode))
        sql_node = TimedeltaExtractNode(
            context=context,
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

    def value_with_unit(self, new_unit: TimedeltaSupportedUnitType) -> Expression:
        """
        Return a numeric expression that represents the timedelta in a new time unit

        Parameters
        ----------
        new_unit : TimedeltaSupportedUnitType
            New time unit

        Returns
        -------
        Expression
        """
        return TimedeltaExtractNode.convert_timedelta_unit(
            self.value_expr.sql, input_unit=self.unit, output_unit=new_unit
        )

    @classmethod
    def build(cls, context: SQLNodeContext) -> TimedeltaNode:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        sql_node = TimedeltaNode(
            context=context,
            table_node=table_node,
            value_expr=input_expr_node,
            unit=context.parameters["unit"],
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
                self.timedelta_node.value_with_unit("microsecond"),
                self.input_date_node.sql,
            ]
        elif isinstance(self.timedelta_node, DateDiffNode):
            # timedelta is the result of date difference
            date_add_args = [
                self.timedelta_node.total_microseconds(),
                self.input_date_node.sql,
            ]
        else:
            # timedelta is a constant value
            quantity_expr = TimedeltaExtractNode.convert_timedelta_unit(
                self.timedelta_node.sql, input_unit="second", output_unit="microsecond"
            )
            date_add_args = [
                quantity_expr,
                self.input_date_node.sql,
            ]
        output_expr = self.context.adapter.dateadd_microsecond(*date_add_args)
        return output_expr

    @classmethod
    def build(cls, context: SQLNodeContext) -> DateAddNode:
        table_node, left_node, right_node = prepare_binary_op_input_nodes(context)
        resolved_timedelta_node = resolve_project_node(right_node)
        assert isinstance(
            resolved_timedelta_node, (TimedeltaNode, DateDiffNode, ParsedExpressionNode)
        )
        output_node = DateAddNode(
            context=context,
            table_node=table_node,
            input_date_node=left_node,
            timedelta_node=resolved_timedelta_node,
        )
        return output_node


@dataclass
class ToTimestampFromEpochNode(ExpressionNode):
    """Node to represent ToTimestampFromEpoch operation"""

    epoch_expr: ExpressionNode
    query_node_type = NodeType.TO_TIMESTAMP_FROM_EPOCH

    @property
    def sql(self) -> Expression:
        return self.context.adapter.from_epoch_seconds(self.epoch_expr.sql)

    @classmethod
    def build(cls, context: SQLNodeContext) -> ToTimestampFromEpochNode:
        input_expr_node = cast(ExpressionNode, context.input_sql_nodes[0])
        table_node = input_expr_node.table_node
        sql_node = ToTimestampFromEpochNode(
            context=context,
            table_node=table_node,
            epoch_expr=input_expr_node,
        )
        return sql_node
