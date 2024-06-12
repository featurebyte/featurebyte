"""
Module for tiles related utilities
"""

from __future__ import annotations

from typing import Any, Optional, Tuple, cast

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.enum import InternalName
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.datetime import TimedeltaExtractNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.interpreter import TileGenSql


def calculate_first_and_last_tile_indices(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    window_size: Optional[int],
    offset: Optional[int],
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
    offset : int
        Feature window offset
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
        offset=offset,
    )
    if window_size is not None:
        num_tiles = window_size // frequency
        first_tile_index_expr = expressions.Sub(
            this=last_tile_index_expr, expression=make_literal_value(num_tiles)
        )
    else:
        first_tile_index_expr = None
    return first_tile_index_expr, last_tile_index_expr


def calculate_last_tile_index_expr(
    adapter: BaseAdapter,
    point_in_time_expr: Expression,
    frequency: int,
    time_modulo_frequency: int,
    offset: Optional[int],
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
    offset : Optional[int]
        Feature window offset

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
    if offset is not None:
        offset_num_tiles = offset // frequency
        last_tile_index_expr = expressions.Sub(
            this=last_tile_index_expr, expression=make_literal_value(offset_num_tiles)
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


def get_previous_job_epoch_expr(
    point_in_time_epoch_expr: Expression, tile_info: TileGenSql
) -> Expression:
    """Get the SQL expression for the epoch second of previous feature job

    Parameters
    ----------
    point_in_time_epoch_expr : Expression
        Expression for point-in-time in epoch second
    tile_info : TileGenSql
        Tile table information

    Returns
    -------
    str
    """
    frequency = make_literal_value(tile_info.frequency)
    time_modulo_frequency = make_literal_value(tile_info.time_modulo_frequency)

    # FLOOR((POINT_IN_TIME - TIME_MODULO_FREQUENCY) / FREQUENCY)
    previous_job_index_expr = expressions.Floor(
        this=expressions.Div(
            this=expressions.Paren(
                this=expressions.Sub(
                    this=point_in_time_epoch_expr, expression=time_modulo_frequency
                )
            ),
            expression=frequency,
        )
    )

    # PREVIOUS_JOB_INDEX * FREQUENCY + TIME_MODULO_FREQUENCY
    previous_job_epoch_expr = expressions.Add(
        this=expressions.Mul(this=previous_job_index_expr, expression=frequency),
        expression=time_modulo_frequency,
    )

    return previous_job_epoch_expr


def get_earliest_tile_start_date_expr(
    adapter: BaseAdapter,
    time_modulo_frequency: Expression,
    blind_spot: Expression,
) -> Expression:
    """
    Get the SQL expression for the earliest tile start date based on feature job settings

    Parameters
    ----------
    adapter: BaseAdapter
        SQL adapter for generating engine specific SQL expressions
    time_modulo_frequency : Expression
        Expression for time modulo frequency in feature job settings
    blind_spot : Expression
        Expression for blind spot in feature job settings

    Returns
    -------
    Expression
    """
    # DATEADD(s, TIME_MODULO_FREQUENCY - BLIND_SPOT, CAST('1970-01-01' AS TIMESTAMP))
    tile_boundaries_offset = expressions.Paren(
        this=expressions.Sub(this=time_modulo_frequency, expression=blind_spot)
    )
    tile_boundaries_offset_microsecond = TimedeltaExtractNode.convert_timedelta_unit(
        tile_boundaries_offset, "second", "microsecond"
    )
    return adapter.dateadd_microsecond(
        tile_boundaries_offset_microsecond,
        cast(Expression, parse_one("CAST('1970-01-01' AS TIMESTAMP)")),
    )


def construct_entity_table_query(
    tile_info: TileGenSql,
    entity_source_expr: expressions.Select,
    start_date_expr: Expression,
    end_date_expr: Expression,
) -> expressions.Select:
    """
    Construct the entity table query that will be used to filter the source table (e.g. EventTable)
    before building tiles. The start and end dates are to be provided, and this function is
    responsible for constructing the table in the expected format.

    Parameters
    ----------
    tile_info : TileGenSql
        Tile table information
    entity_source_expr : expressions.Select
        Table from which the entity table will be constructed
    start_date_expr : Expression
        Expression for the tile start date
    end_date_expr : Expression
        Expression for the tile end date

    Returns
    -------
    expressions.Select
    """

    # Tile compute sql uses original table columns instead of serving names
    serving_names_to_keys = [
        f"{quoted_identifier(serving_name).sql()} AS {quoted_identifier(col).sql()}"
        for serving_name, col in zip(tile_info.serving_names, tile_info.entity_columns)
    ]

    # This is the groupby keys used to construct the entity table
    serving_names = [f"{quoted_identifier(col).sql()}" for col in tile_info.serving_names]

    entity_table_expr = entity_source_expr.select(
        *serving_names_to_keys,
        expressions.alias_(end_date_expr, InternalName.ENTITY_TABLE_END_DATE.value),
        expressions.alias_(start_date_expr, InternalName.ENTITY_TABLE_START_DATE.value),
    ).group_by(*serving_names)

    return entity_table_expr
