"""
Module to handle partition filters in SQL queries
"""

from typing import Optional

from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import PartitionColumnFilter, quoted_identifier


def get_partition_filter(
    partition_column: str | Expression,
    partition_column_filter: PartitionColumnFilter,
    format_string: Optional[str],
    adapter: BaseAdapter,
) -> Expression:
    """
    Generate a partition filter expression

    Parameters
    ----------
    partition_column: str | Expression
        The name of the partition column.
    partition_column_filter: PartitionColumnFilter
        The partition column filter containing the from and to timestamps
    format_string: Optional[str]
        Format string for the timestamp, if applicable.
    adapter: BaseAdapter
        The SQL adapter to use for generating the expression.

    Returns
    -------
    Expression
        The SQL expression representing the partition filter.
    """

    def _get_adjusted_boundary_value(value: Expression, backward: bool) -> Expression:
        """
        Adjust the boundary value based on the buffer defined in the partition column filter.

        Parameters
        ----------
        value: Expression
            The boundary value to adjust.
        backward: bool
            Whether this is the backward adjustment (i.e., subtracting the buffer).

        Returns
        -------
        Expression
            The adjusted boundary value.
        """
        buffer = partition_column_filter.buffer
        if not buffer:
            return value
        adjusted_value = adapter.dateadd_time_interval(
            quantity_expr=make_literal_value(
                buffer.value if not backward else -buffer.value,
            ),
            unit=buffer.unit,
            timestamp_expr=value,
        )
        return adjusted_value

    def _get_boundary_value_expr(value: Expression) -> Expression:
        """
        Get the SQL expression for the boundary value, accounting for any format string.

        Parameters
        ----------
        value: Expression
            The boundary value to convert into an SQL expression.

        Returns
        -------
        Expression
            The SQL expression representing the boundary value, formatted if necessary.
        """
        if format_string is not None:
            # If a format string is provided, use it to format the timestamp
            value = adapter.format_timestamp(value, format_string)
        return value

    from_timestamp = partition_column_filter.from_timestamp
    to_timestamp = partition_column_filter.to_timestamp
    assert from_timestamp is not None or to_timestamp is not None, (
        "At least one of from_timestamp or to_timestamp must be provided"
    )

    if isinstance(partition_column, str):
        partition_column_expr = quoted_identifier(partition_column)
    else:
        partition_column_expr = partition_column
    conditions: list[Expression] = []
    if from_timestamp is not None:
        conditions.append(
            expressions.GTE(
                this=partition_column_expr,
                expression=_get_boundary_value_expr(
                    _get_adjusted_boundary_value(from_timestamp, backward=True)
                ),
            )
        )
    if to_timestamp is not None:
        conditions.append(
            expressions.LTE(
                this=partition_column_expr,
                expression=_get_boundary_value_expr(
                    _get_adjusted_boundary_value(to_timestamp, backward=False)
                ),
            )
        )
    return expressions.and_(*conditions)
