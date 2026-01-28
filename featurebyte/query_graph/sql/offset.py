"""
Sql helper related to offset
"""

from __future__ import annotations

import pandas as pd
from sqlglot.expressions import Expression

from featurebyte.enum import StrEnum
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value


class OffsetDirection(StrEnum):
    """
    Offset direction
    """

    FORWARD = "forward"
    BACKWARD = "backward"


def add_offset_to_timestamp(
    adapter: BaseAdapter,
    timestamp_expr: Expression,
    offset: str,
    offset_direction: OffsetDirection = OffsetDirection.BACKWARD,
) -> Expression:
    """
    Returns an expression for the timestamp adjusted by offset

    Parameters
    ----------
    adapter: BaseAdapter
        Sql adapter
    timestamp_expr: Expression
        Timestamp expression to adjust
    offset: str
        Offset specification
    offset_direction: OffsetDirection
        Offset direction

    Returns
    -------
    Expression
    """
    offset_seconds = pd.Timedelta(offset).total_seconds()
    direction_adjustment_multiplier = -1 if offset_direction == OffsetDirection.BACKWARD else 1
    adjusted_timestamp_expr = adapter.dateadd_microsecond(
        make_literal_value(offset_seconds * 1e6 * direction_adjustment_multiplier),
        timestamp_expr,
    )
    return adjusted_timestamp_expr


def adjust_point_in_time_for_offset(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    offset: str | None,
    offset_direction: OffsetDirection,
) -> Expression:
    """
    Adjust point in time expression for offset and normalize for comparison.

    Parameters
    ----------
    adapter: BaseAdapter
        Sql adapter
    point_in_time_expr: Expression
        Point in time expression to adjust
    offset: str | None
        Offset specification
    offset_direction: OffsetDirection
        Offset direction

    Returns
    -------
    Expression
    """
    if offset is not None:
        point_in_time_expr = add_offset_to_timestamp(
            adapter=adapter,
            timestamp_expr=point_in_time_expr,
            offset=offset,
            offset_direction=offset_direction,
        )
    return adapter.normalize_timestamp_before_comparison(point_in_time_expr)
