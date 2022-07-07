"""
On-demand tile computation for feature preview
"""
from __future__ import annotations

import pandas as pd

from featurebyte.enum import InternalName
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter, TileGenSql


def construct_on_demand_tile_ctes(
    graph: QueryGraph,
    node: Node,
    point_in_time: str,
) -> list[tuple[str, str]]:
    """Construct SQL that computes tiles required for feature preview

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node
    point_in_time : str
        Point in time

    Returns
    -------
    list[tuple[str, str]]
    """
    tile_gen_info_lst = get_tile_gen_info(graph, node)
    cte_statements = []

    for tile_info in tile_gen_info_lst:
        # Convert template SQL with concrete start and end timestamps, based on the requested
        # point-in-time and feature window sizes
        tile_sql_with_start_end = get_tile_sql_from_point_in_time(
            sql_template=tile_info.sql,
            point_in_time=point_in_time,
            frequency=tile_info.frequency,
            time_modulo_frequency=tile_info.time_modulo_frequency,
            blind_spot=tile_info.blind_spot,
            windows=tile_info.windows,
        )
        # Include global tile index that would have been computed by F_TIMESTAMP_TO_INDEX UDF during
        # scheduled tile jobs
        final_tile_sql = get_tile_sql_parameterized_by_job_settings(
            tile_sql_with_start_end,
            frequency=tile_info.frequency,
            time_modulo_frequency=tile_info.time_modulo_frequency,
            blind_spot=tile_info.blind_spot,
        )
        tile_table_id = tile_info.tile_table_id
        cte_statements.append((tile_table_id, final_tile_sql))

    return cte_statements


def get_tile_gen_info(graph: QueryGraph, node: Node) -> list[TileGenSql]:
    """Construct TileGenSql that contains recipe of building tiles

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node

    Returns
    -------
    list[TileGenSql]
    """
    interpreter = GraphInterpreter(graph)
    tile_gen_info = interpreter.construct_tile_gen_sql(node)
    return tile_gen_info


def get_epoch_seconds(datetime_like: str) -> int:
    """Convert datetime string to UNIX timestamp

    Parameters
    ----------
    datetime_like : str
        Datetime string to be converted

    Returns
    -------
    int
        Converted UNIX timestamp
    """
    return int(pd.to_datetime(datetime_like).timestamp())


def epoch_seconds_to_timestamp(num_seconds: int) -> pd.Timestamp:
    """Convert UNIX timestamp to pandas Timestamp

    Parameters
    ----------
    num_seconds : int
        Number of seconds from epoch to be converted

    Returns
    -------
    pd.Timestamp
    """
    return pd.Timestamp(num_seconds, unit="s")


def compute_start_end_date_from_point_in_time(
    point_in_time: str,
    frequency: int,
    time_modulo_frequency: int,
    blind_spot: int,
    num_tiles: int,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Compute start and end dates to fill in the placeholders in tile SQL template

    Parameters
    ----------
    point_in_time : str
        Point in time. This will determine the range of data required to build tiles
    frequency : int
        Frequency in feature job setting
    time_modulo_frequency : int
        Time modulo frequency in feature job setting
    blind_spot : int
        Blind spot in feature job setting
    num_tiles : int
        Number of tiles required. This is calculated from feature window size.

    Returns
    -------
    tuple[pd.Timestamp, pd.Timestamp]
        Tuple of start and end dates
    """
    # Calculate the time of the latest feature job before point in time
    point_in_time_epoch_seconds = get_epoch_seconds(point_in_time)
    last_job_index = (point_in_time_epoch_seconds - time_modulo_frequency) // frequency
    last_job_time_epoch_seconds = last_job_index * frequency + time_modulo_frequency

    # Compute start and end dates based on number of tiles required
    end_date_epoch_seconds = last_job_time_epoch_seconds - blind_spot
    start_date_epoch_seconds = end_date_epoch_seconds - num_tiles * frequency
    start_date = epoch_seconds_to_timestamp(start_date_epoch_seconds)
    end_date = epoch_seconds_to_timestamp(end_date_epoch_seconds)

    return start_date, end_date


def get_tile_sql_from_point_in_time(
    sql_template: str,
    point_in_time: str,
    frequency: int,
    time_modulo_frequency: int,
    blind_spot: int,
    windows: list[int],
) -> str:
    """Fill in start date and end date placeholders for template tile SQL

    Parameters
    ----------
    sql_template : str
        Tile SQL template
    point_in_time : str
        Point in time in the request
    frequency : int
        Frequency in feature job setting
    time_modulo_frequency : int
        Time modulo frequency in feature job setting
    blind_spot : int
        Blind spot in feature job setting
    windows : list[int]
        List of window sizes

    Returns
    -------
    sql
    """
    max_window = max(pd.Timedelta(x).total_seconds() for x in windows)
    assert max_window % frequency == 0
    num_tiles = int(max_window // frequency)
    start_date, end_date = compute_start_end_date_from_point_in_time(
        point_in_time,
        frequency=frequency,
        time_modulo_frequency=time_modulo_frequency,
        blind_spot=blind_spot,
        num_tiles=num_tiles,
    )
    sql = sql_template
    sql = sql.replace(InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_date}'")
    sql = sql.replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_date}'")
    return sql


def get_tile_sql_parameterized_by_job_settings(
    tile_sql: str,
    frequency: int,
    time_modulo_frequency: int,
    blind_spot: int,
) -> str:
    """Get the final tile SQL code that would have been executed by the tile schedule job

    The template SQL code doesn't contain the tile index, which is computed by the tile schedule job
    by F_TIMESTAMP_TO_INDEX. This produces a SQL that incorporates that.

    Parameters
    ----------
    tile_sql : str
        Tile SQL with placeholders alrady filled
    frequency : int
        Frequency in feature job setting
    time_modulo_frequency : int
        Time modulo frequency in feature job setting
    blind_spot : int
        Blind spot in feature job setting

    Returns
    -------
    str
    """
    frequency_minute = frequency // 60
    index_expr = (
        f"F_TIMESTAMP_TO_INDEX("
        f"  {InternalName.TILE_START_DATE},"
        f"  {time_modulo_frequency},"
        f"  {blind_spot},"
        f"  {frequency_minute}"
        f")"
    )
    sql = f"""
    SELECT *, {index_expr} AS "INDEX" FROM (
        {tile_sql}
    )
    """
    return sql
