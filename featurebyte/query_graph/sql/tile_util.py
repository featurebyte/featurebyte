"""
Module for tiles related utilities
"""
from __future__ import annotations

from typing import Tuple, cast

from sqlglot import parse_one
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.adapter import BaseAdapter


def calculate_first_and_last_tile_indices(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    window_size: int,
    frequency: int,
    time_modulo_frequency: int,
) -> Tuple[Expression, Expression]:
    """
    Calculate the first (inclusive) and last (exclusive) tile indices required to compute a feature,
    given:

    1) point in time
    2) feature window size
    3) feature job settings

    Feature value is the result of aggregating tiles within this range.

    Parameters
    ----------
    adapter: BaseAdapter
        SQL adapter for generating engine specific SQL expressions
    point_in_time_expr: Expression
        Expression for the point in time column
    window_size : int
        Feature window size
    frequency : int
        Frequency in feature job setting
    time_modulo_frequency : int
        Time modulo frequency in feature job setting

    Returns
    -------
    Tuple[Expression, Expression]
    """
    num_tiles = window_size // frequency
    point_in_time_epoch_expr = adapter.to_epoch_seconds(point_in_time_expr)
    last_tile_index_expr = cast(
        Expression,
        parse_one(
            f"FLOOR(({point_in_time_epoch_expr.sql()} - {time_modulo_frequency}) / {frequency})"
        ),
    )
    first_tile_index_expr = cast(
        Expression, parse_one(f"{last_tile_index_expr.sql()} - {num_tiles}")
    )
    return first_tile_index_expr, last_tile_index_expr
