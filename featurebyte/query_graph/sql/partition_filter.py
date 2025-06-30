"""
Module to handle partition filters in SQL queries
"""

from datetime import datetime
from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier


def get_partition_filter(
    partition_column: str,
    from_timestamp: Optional[datetime],
    to_timestamp: Optional[datetime],
    format_string: Optional[str],
    adapter: BaseAdapter,
) -> Expression:
    """
    Generate a partition filter expression

    Parameters
    ----------
    partition_column: str
        The name of the partition column.
    from_timestamp: Optional[datetime]
        The start timestamp for the filter.
    to_timestamp: Optional[datetime]
        The end timestamp for the filter.
    format_string: Optional[str]
        Format string for the timestamp, if applicable.
    adapter: BaseAdapter
        The SQL adapter to use for generating the expression.

    Returns
    -------
    Expression
        The SQL expression representing the partition filter.
    """

    def _get_boundary_value_expr(value: datetime) -> Optional[Expression]:
        expr = make_literal_value(value, cast_as_timestamp=True)
        if format_string is not None:
            # If a format string is provided, use it to format the timestamp
            expr = adapter.format_timestamp(expr, format_string)
        return expr

    assert (
        from_timestamp is not None or to_timestamp is not None
    ), "At least one of from_timestamp or to_timestamp must be provided"

    partition_column_expr = quoted_identifier(partition_column)
    conditions: list[Expression] = []
    if from_timestamp is not None:
        conditions.append(
            expressions.GTE(
                this=partition_column_expr,
                expression=_get_boundary_value_expr(from_timestamp),
            )
        )
    if to_timestamp is not None:
        conditions.append(
            expressions.LTE(
                this=partition_column_expr,
                expression=_get_boundary_value_expr(to_timestamp),
            )
        )
    return expressions.and_(*conditions)
