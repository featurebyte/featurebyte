"""
Module for tiles related utilities
"""
from __future__ import annotations

from typing import Tuple, cast

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.literal import make_literal_value


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
    last_tile_index_expr = calculate_last_tile_index_expr(
        adapter=adapter,
        point_in_time_expr=point_in_time_expr,
        frequency=frequency,
        time_modulo_frequency=time_modulo_frequency,
    )
    num_tiles = window_size // frequency
    first_tile_index_expr = cast(
        Expression, parse_one(f"{last_tile_index_expr.sql()} - {num_tiles}")
    )
    return first_tile_index_expr, last_tile_index_expr


def calculate_last_tile_index_expr(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    frequency: int,
    time_modulo_frequency: int,
    exclusive: bool = True,
):
    """
    Calculate the last tile index required to compute a feature

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
    exclusive : bool
        Whether the calculated tile index is exclusive (i.e. should be excluded when merging tiles)
    """
    point_in_time_epoch_expr = adapter.to_epoch_seconds(point_in_time_expr)
    last_tile_index_expr = cast(
        Expression,
        parse_one(
            f"FLOOR(({point_in_time_epoch_expr.sql()} - {time_modulo_frequency}) / {frequency})"
        ),
    )
    if not exclusive:
        last_tile_index_expr = expressions.Sub(
            this=last_tile_index_expr, expression=make_literal_value(1)
        )
    return last_tile_index_expr
