"""
Module to handle partition filters in SQL queries
"""

from datetime import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta
from sqlglot import expressions
from sqlglot.expressions import Expression

from featurebyte.enum import TimeIntervalUnit
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import PartitionColumnFilter, quoted_identifier


def convert_time_interval_to_relativedelta(
    time_interval: TimeInterval, backward: bool
) -> relativedelta:
    """
    Convert a TimeInterval to a relativedelta object.

    Parameters
    ----------
    time_interval: TimeInterval
        The time interval to convert.
    backward: bool
        If True, the relativedelta will be negative (i.e., it represents a backward interval).

    Returns
    -------
    relativedelta
        The corresponding relativedelta object.
    """
    # Map TimeInterval units to relativedelta arguments. We only use this internally, so we
    # only support the required units for now.
    assert time_interval.unit in [
        TimeIntervalUnit.DAY,
        TimeIntervalUnit.MONTH,
    ], f"Unsupported time interval unit: {time_interval.unit}"
    if backward:
        value = -time_interval.value
    else:
        value = time_interval.value
    if time_interval.unit == TimeIntervalUnit.DAY:
        return relativedelta(days=value)
    return relativedelta(months=value)


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

    def _get_adjusted_boundary_value(
        value: datetime | Expression, backward: bool
    ) -> datetime | Expression:
        """
        Adjust the boundary value based on the buffer defined in the partition column filter.

        Parameters
        ----------
        value: datetime | Expression
            The boundary value to adjust.
        backward: bool
            Whether this is the backward adjustment (i.e., subtracting the buffer).

        Returns
        -------
        datetime | Expression
            The adjusted boundary value.
        """
        buffer = partition_column_filter.buffer
        if not buffer:
            return value
        adjusted_value: datetime | Expression
        if isinstance(value, datetime):
            adjusted_value = value + convert_time_interval_to_relativedelta(buffer, backward)
        else:
            adjusted_value = adapter.dateadd_time_interval(
                quantity_expr=make_literal_value(
                    buffer.value if not backward else -buffer.value,
                ),
                unit=buffer.unit,
                timestamp_expr=value,
            )
        return adjusted_value

    def _get_boundary_value_expr(value: datetime | Expression) -> Expression:
        """
        Get the SQL expression for the boundary value, accounting for any format string.

        Parameters
        ----------
        value: datetime | Expression
            The boundary value to convert into an SQL expression.

        Returns
        -------
        Expression
            The SQL expression representing the boundary value, formatted if necessary.
        """
        if isinstance(value, Expression):
            # E.g. a placeholder in scheduled tile sql
            expr = value
        else:
            expr = make_literal_value(value, cast_as_timestamp=True)
        if format_string is not None:
            # If a format string is provided, use it to format the timestamp
            expr = adapter.format_timestamp(expr, format_string)
        return expr

    from_timestamp = partition_column_filter.from_timestamp
    to_timestamp = partition_column_filter.to_timestamp
    assert (
        from_timestamp is not None or to_timestamp is not None
    ), "At least one of from_timestamp or to_timestamp must be provided"

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
