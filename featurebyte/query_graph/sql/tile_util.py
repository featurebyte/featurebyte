"""
Module for tiles related utilities
"""

from __future__ import annotations

from typing import Any, Literal, Optional, Tuple

from sqlglot import expressions, parse_one
from sqlglot.expressions import Expression

from featurebyte.common.model_util import parse_duration_string
from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.sql.adapter import BaseAdapter
from featurebyte.query_graph.sql.ast.datetime import TimedeltaExtractNode
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.feature_job import get_previous_job_epoch_expr_from_settings
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
    last_tile_index_expr: Expression
    last_tile_index_expr = expressions.Cast(
        this=expressions.Floor(
            this=expressions.Div(
                this=expressions.Paren(
                    this=expressions.Sub(
                        this=point_in_time_epoch_expr,
                        expression=make_literal_value(time_modulo_frequency),
                    )
                ),
                expression=make_literal_value(frequency),
            ),
        ),
        to=expressions.DataType.build("BIGINT"),
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


def get_max_window_sizes(
    tile_info_list: list[TileGenSql],
    key_name: Literal["tile_table_id", "aggregation_id"],
) -> dict[str, Optional[int]]:
    """
    Get a dictionary of maximum window sizes per tile table or aggregation id

    Parameters
    ----------
    tile_info_list: list[TileGenSql]
        Tile table information
    key_name: Literal["tile_table_id", "aggregation_id"]
        Key name to use for grouping

    Returns
    -------
    dict[str, Optional[int]]
    """
    max_window_sizes: dict[str, Optional[int]] = {}
    for tile_info in tile_info_list:
        if key_name == "tile_table_id":
            key = tile_info.tile_table_id
        else:
            key = tile_info.aggregation_id
        for window in tile_info.windows:
            if window is not None:
                window_size = parse_duration_string(window)
                if tile_info.offset is not None:
                    window_size += parse_duration_string(tile_info.offset)
                assert window_size % tile_info.frequency == 0
            else:
                window_size = None
            update_maximum_window_size_dict(
                max_window_size_dict=max_window_sizes,
                key=key,
                window_size=window_size,
            )
    return max_window_sizes


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
    Expression
    """
    return get_previous_job_epoch_expr_from_settings(
        point_in_time_epoch_expr=point_in_time_epoch_expr,
        period_seconds=tile_info.frequency,
        offset_seconds=tile_info.time_modulo_frequency,
    )


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
        parse_one("CAST('1970-01-01' AS TIMESTAMP)"),
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


def construct_entity_table_query_for_window(
    adapter: BaseAdapter,
    tile_info: TileGenSql,
    request_table_name: str,
    window: Optional[int],
) -> expressions.Select:
    """
    Construct entity table query from TIleInfo and window

    Parameters
    ----------
    adapter : BaseAdapter
        Instance of BaseAdapter for generating engine specific SQL
    tile_info : TileGenSql
        Tile table information
    request_table_name : str
        Name of the request table
    window : Optional[int]
        Window size in seconds. None for features with an unbounded window.

    Returns
    -------
    expressions.Select
    """

    def get_tile_boundary(point_in_time_expr: Expression) -> Expression:
        previous_job_epoch_expr = get_previous_job_epoch_expr(
            adapter.to_epoch_seconds(point_in_time_expr), tile_info
        )
        return adapter.from_epoch_seconds(
            expressions.Sub(this=previous_job_epoch_expr, expression=blind_spot)
        )

    blind_spot = make_literal_value(tile_info.blind_spot)
    time_modulo_frequency = make_literal_value(tile_info.time_modulo_frequency)

    if window:
        num_tiles = int(window // tile_info.frequency)
    else:
        num_tiles = None

    # Tile end date is determined from the latest point in time per entity
    end_date_expr = get_tile_boundary(
        expressions.Max(this=expressions.Identifier(this=SpecialColumnName.POINT_IN_TIME.value))
    )

    if num_tiles:
        minus_num_tiles_in_microseconds = expressions.Mul(
            this=TimedeltaExtractNode.convert_timedelta_unit(
                expressions.Mul(
                    this=make_literal_value(num_tiles),
                    expression=make_literal_value(tile_info.frequency),
                ),
                "second",
                "microsecond",
            ),
            expression=make_literal_value(-1),
        )
        # Tile start date is determined from the earliest point in time per entity minus the largest
        # feature window
        start_date_expr = adapter.dateadd_microsecond(
            minus_num_tiles_in_microseconds,
            get_tile_boundary(
                expressions.Min(
                    this=expressions.Identifier(this=SpecialColumnName.POINT_IN_TIME.value)
                )
            ),
        )
    else:
        start_date_expr = get_earliest_tile_start_date_expr(
            adapter=adapter,
            time_modulo_frequency=time_modulo_frequency,
            blind_spot=blind_spot,
        )

    entity_table_expr = construct_entity_table_query(
        tile_info=tile_info,
        entity_source_expr=expressions.select().from_(quoted_identifier(request_table_name)),
        start_date_expr=start_date_expr,
        end_date_expr=end_date_expr,
    )
    return entity_table_expr
