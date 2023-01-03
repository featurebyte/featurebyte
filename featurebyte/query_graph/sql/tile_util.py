"""
Module for tiles related utilities
"""
from __future__ import annotations

from typing import Any, Optional, Tuple, cast

from sqlglot import parse_one
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.adapter import BaseAdapter


def calculate_first_and_last_tile_indices(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    window_size: Optional[int],
    frequency: int,
    time_modulo_frequency: int,
) -> Tuple[Optional[Expression], Expression]:
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
    last_tile_index_expr = calculate_last_tile_index_expr(
        adapter=adapter,
        point_in_time_expr=point_in_time_expr,
        frequency=frequency,
        time_modulo_frequency=time_modulo_frequency,
    )
    if window_size is not None:
        num_tiles = window_size // frequency
        first_tile_index_expr = cast(
            Expression, parse_one(f"{last_tile_index_expr.sql()} - {num_tiles}")
        )
    else:
        first_tile_index_expr = None
    return first_tile_index_expr, last_tile_index_expr


def calculate_last_tile_index_expr(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    frequency: int,
    time_modulo_frequency: int,
) -> Expression:
    """
    Calculate the last tile index (exclusive) required to compute a feature

    Parameters
    ----------
    adapter: BaseAdapter
        SQL adapter for generating engine specific SQL expressions
    point_in_time_expr: Expression
        Expression for the point in time column
    frequency : int
        Frequency in feature job setting
    time_modulo_frequency : int
        Time modulo frequency in feature job setting

    Returns
    -------
    Expression
    """
    point_in_time_epoch_expr = adapter.to_epoch_seconds(point_in_time_expr)
    last_tile_index_expr = cast(
        Expression,
        parse_one(
            f"FLOOR(({point_in_time_epoch_expr.sql()} - {time_modulo_frequency}) / {frequency})"
        ),
    )
    return last_tile_index_expr


def update_maximum_window_size_dict(
    max_window_size_dict: dict[Any, int | None],
    key: Any,
    window_size: int | None,
) -> None:
    """
    Update a dictionary that keeps track of maximum window size per certain key

    Window size of None is considered as larger than any concrete window size.

    Parameters
    ----------
    max_window_size_dict: dict[Any, int | None]
        Dictionary to update
    key: Any
        Key of interest to update
    window_size: int | None
        Window size in seconds
    """
    if key not in max_window_size_dict:
        max_window_size_dict[key] = window_size
    else:
        existing_window_size = max_window_size_dict[key]

        # Existing window size of None means that the window should be unbounded; in that
        # case window size comparison is irrelevant and should be skipped
        if existing_window_size is None:
            return

        if window_size is None:
            max_window_size_dict[key] = None
        else:
            max_window_size_dict[key] = max(existing_window_size, window_size)
