"""
Sql helper related to offset
"""
from __future__ import annotations

import pandas as pd
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.scd_helper import OffsetDirection


def add_offset_to_timestamp(
    adapter: BaseAdapter,
    timestamp_expr: Expression,
    offset: str,
    offset_direction: OffsetDirection,
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
