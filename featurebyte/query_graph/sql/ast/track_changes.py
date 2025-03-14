"""
Module for TRACK_CHANGES node sql generation
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from sqlglot import expressions
from sqlglot.expressions import Expression, Select, alias_, select

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.node.generic import TrackChangesNodeParameters
from featurebyte.query_graph.sql.ast.base import SQLNodeContext, TableNode
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc


def create_lag_expression(
    partition_key_expr: Expression,
    timestamp_column_expr: Expression,
    column_expr: Expression,
) -> Expression:
    """
    Create a LAG expression

    Parameters
    ----------
    partition_key_expr: Expression
        Expression for the partition key
    timestamp_column_expr: Expression
        Expression for the timestamp column
    column_expr: Expression
        Expression for the column to be lagged

    Returns
    -------
    Expression
    """
    partition_by = [partition_key_expr]
    order = expressions.Order(expressions=[expressions.Ordered(this=timestamp_column_expr)])
    return expressions.Window(
        this=expressions.Anonymous(this="LAG", expressions=[column_expr]),
        partition_by=partition_by,
        order=order,
    )


@dataclass
class TrackChanges(TableNode):
    """
    TrackChanges SQLNode
    """

    track_changes_subquery: Expression
    query_node_type = NodeType.TRACK_CHANGES

    def from_query_impl(self, select_expr: Select) -> Select:
        return select_expr.from_(self.track_changes_subquery)

    @classmethod
    def build(cls, context: SQLNodeContext) -> TrackChanges:
        parameters = TrackChangesNodeParameters(**context.parameters)
        input_node = cast(TableNode, context.input_sql_nodes[0])

        # Previous value column
        effective_timestamp_expr = quoted_identifier(parameters.effective_timestamp_column)
        if parameters.effective_timestamp_schema is not None:
            effective_timestamp_expr = convert_timestamp_to_utc(
                effective_timestamp_expr,
                parameters.effective_timestamp_schema,
                context.adapter,
            )
        previous_value = create_lag_expression(
            partition_key_expr=quoted_identifier(parameters.natural_key_column),
            timestamp_column_expr=effective_timestamp_expr,
            column_expr=quoted_identifier(parameters.tracked_column),
        )

        # Condition on whether the value has changed
        value_changed_condition = expressions.NullSafeNEQ(
            this=quoted_identifier(parameters.tracked_column), expression=previous_value
        )

        # This query includes only the rows where the tracked column has changed
        filtered_input_with_changes = context.adapter.filter_with_window_function(
            select(
                quoted_identifier(parameters.natural_key_column),
                alias_(
                    effective_timestamp_expr,
                    alias=parameters.new_valid_from_column_name,
                    quoted=True,
                ),
                alias_(
                    quoted_identifier(parameters.tracked_column),
                    alias=parameters.new_tracked_column_name,
                    quoted=True,
                ),
                alias_(previous_value, alias=parameters.previous_tracked_column_name, quoted=True),
            ).from_(input_node.sql_nested()),
            [
                parameters.natural_key_column,
                parameters.new_valid_from_column_name,
                parameters.new_tracked_column_name,
                parameters.previous_tracked_column_name,
            ],
            value_changed_condition,
        )

        # The final step is to calculate the previous_valid_from_column. This is a lag expression
        # that operates on the filtered view above.
        previous_valid_from_expr = create_lag_expression(
            partition_key_expr=quoted_identifier(parameters.natural_key_column),
            timestamp_column_expr=quoted_identifier(parameters.new_valid_from_column_name),
            column_expr=quoted_identifier(parameters.new_valid_from_column_name),
        )
        track_changes_subquery = (
            select(
                quoted_identifier(parameters.natural_key_column),
                quoted_identifier(parameters.new_valid_from_column_name),
                quoted_identifier(parameters.new_tracked_column_name),
                alias_(
                    previous_valid_from_expr,
                    alias=parameters.previous_valid_from_column_name,
                    quoted=True,
                ),
                quoted_identifier(parameters.previous_tracked_column_name),
            )
            .from_(filtered_input_with_changes.subquery())
            .subquery()
        )

        columns_map = {
            str(column_name): quoted_identifier(column_name)
            for column_name in [
                parameters.natural_key_column,
                parameters.new_valid_from_column_name,
                parameters.previous_valid_from_column_name,
                parameters.new_tracked_column_name,
                parameters.previous_tracked_column_name,
            ]
        }

        return TrackChanges(
            context=context,
            columns_map=columns_map,
            track_changes_subquery=track_changes_subquery,
        )
