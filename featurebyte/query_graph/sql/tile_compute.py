"""
On-demand tile computation for feature preview
"""
from __future__ import annotations

import pandas as pd
from sqlglot import expressions, parse_one, select

from featurebyte.enum import InternalName, SourceType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.common import escape_column_name
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql


class OnDemandTileComputePlan:
    """Responsible for generating SQL to compute tiles for preview purpose

    Feature preview uses the same SQL query as historical feature requests. As a result, we need to
    build temporary tile tables that are required by the feature query. Actual tile tables are wide
    and consist of tile values from different transforms (aggregation_id). Based on the current
    implementation, for feature preview each groupby node has its own tile SQLs, so we need to
    perform some manipulation to construct the wide tile tables.

    Parameters
    ----------
    point_in_time : str
        Point in time value specified when calling preview
    """

    def __init__(self, point_in_time: str, source_type: SourceType):
        self.point_in_time = point_in_time
        self.processed_agg_ids: set[str] = set()
        self.max_window_size_by_agg_id: dict[str, int] = {}
        self.tile_infos: list[TileGenSql] = []
        self.source_type = source_type

    def process_node(self, graph: QueryGraph, node: Node) -> None:
        """Update state given a query graph node

        Parameters
        ----------
        graph : QueryGraph
            Query graph
        node : Node
            Query graph node
        """
        tile_gen_info_lst = get_tile_gen_info(graph, node, self.source_type)

        for tile_info in tile_gen_info_lst:

            # The date range of each tile table depends on the feature window sizes.
            self.update_max_window_size(tile_info)

            if tile_info.aggregation_id in self.processed_agg_ids:
                # The same aggregation_id can appear more than once. For example, two groupby
                # operations with the same parameters except windows will have the same
                # aggregation_id.
                continue

            self.tile_infos.append(tile_info)
            self.processed_agg_ids.add(tile_info.aggregation_id)

    def construct_tile_sqls(self) -> dict[str, expressions.Select]:
        """Construct SQL expressions for all the required tile tables

        Returns
        -------
        dict[str, expressions.Expression]
        """

        tile_sqls: dict[str, expressions.Select] = {}
        prev_aliases: dict[str, str] = {}

        for tile_info in self.tile_infos:
            # Convert template SQL with concrete start and end timestamps, based on the requested
            # point-in-time and feature window sizes
            tile_sql_with_start_end = get_tile_sql_from_point_in_time(
                sql_template=tile_info.sql,
                point_in_time=self.point_in_time,
                frequency=tile_info.frequency,
                time_modulo_frequency=tile_info.time_modulo_frequency,
                blind_spot=tile_info.blind_spot,
                window=self.get_max_window_size(tile_info.aggregation_id),
            )
            # Include global tile index that would have been computed by F_TIMESTAMP_TO_INDEX UDF
            # during scheduled tile jobs
            final_tile_sql = get_tile_sql_parameterized_by_job_settings(
                tile_sql_with_start_end,
                frequency=tile_info.frequency,
                time_modulo_frequency=tile_info.time_modulo_frequency,
                blind_spot=tile_info.blind_spot,
            )

            # Build wide tile table by joining tile sqls with the same tile_table_id
            tile_table_id = tile_info.tile_table_id
            agg_id = tile_info.aggregation_id
            tile_sql_expr = parse_one(final_tile_sql)
            assert isinstance(tile_sql_expr, expressions.Subqueryable)

            if tile_table_id not in tile_sqls:
                # New tile table - get the tile index column, entity columns and tile value columns
                keys = [f"{agg_id}.{escape_column_name(key)}" for key in tile_info.entity_columns]
                if tile_info.value_by_column is not None:
                    keys.append(f"{agg_id}.{escape_column_name(tile_info.value_by_column)}")
                tile_sql = select(f"{agg_id}.INDEX", *keys, *tile_info.tile_value_columns).from_(
                    tile_sql_expr.subquery(alias=agg_id)
                )
                tile_sqls[tile_table_id] = tile_sql
                prev_aliases[tile_table_id] = agg_id
            else:
                # Tile table already exist - get the new tile value columns by doing a join. Tile
                # index column and entity columns exist already.
                prev_alias = prev_aliases[tile_table_id]
                join_conditions = [f"{prev_alias}.INDEX = {agg_id}.INDEX"]
                for key in tile_info.entity_columns:
                    key = escape_column_name(key)
                    join_conditions.append(f"{prev_alias}.{key} = {agg_id}.{key}")
                # Tile sqls with the same tile_table_id should generate output with identical set of
                # tile indices and entity columns (they are derived from the same event data using
                # the same entity columns and feature job settings). Any join type will work, but
                # using "right" join allows the filter on entity columns to be pushed down to
                # TableScan in the optimized query.
                tile_sqls[tile_table_id] = (
                    tile_sqls[tile_table_id]
                    .join(
                        tile_sql_expr.subquery(),
                        join_type="right",
                        join_alias=agg_id,
                        on=expressions.and_(*join_conditions),
                    )
                    .select(*tile_info.tile_value_columns)
                )

        return tile_sqls

    def update_max_window_size(self, tile_info: TileGenSql) -> None:
        """Update the maximum feature window size observed for each aggregation_id

        Parameters
        ----------
        tile_info : TileGenSql
            Tile table information
        """
        agg_id = tile_info.aggregation_id
        max_window = max(int(pd.Timedelta(x).total_seconds()) for x in tile_info.windows)
        assert max_window % tile_info.frequency == 0
        if (
            agg_id not in self.max_window_size_by_agg_id
            or max_window > self.max_window_size_by_agg_id[agg_id]
        ):
            self.max_window_size_by_agg_id[agg_id] = max_window

    def get_max_window_size(self, aggregation_id: str) -> int:
        """Get the maximum feature window size for a given aggregation_id

        Parameters
        ----------
        aggregation_id : str
            Aggregation identifier

        Returns
        -------
        int
        """
        return self.max_window_size_by_agg_id[aggregation_id]

    def construct_on_demand_tile_ctes(self) -> list[tuple[str, expressions.Select]]:
        """Construct the CTE statements that would compute all the required tiles

        Returns
        -------
        list[tuple[str, str]]
        """
        cte_statements = []
        tile_sqls = self.construct_tile_sqls()
        for tile_table_id, tile_sql_expr in tile_sqls.items():
            cte_statements.append((tile_table_id, tile_sql_expr))
        return cte_statements


def get_tile_gen_info(graph: QueryGraph, node: Node, source_type: SourceType) -> list[TileGenSql]:
    """Construct TileGenSql that contains recipe of building tiles

    Parameters
    ----------
    graph : QueryGraph
        Query graph
    node : Node
        Query graph node
    source_type : SourceType
        Source type information

    Returns
    -------
    list[TileGenSql]
    """
    interpreter = GraphInterpreter(graph, source_type=source_type)
    tile_gen_info = interpreter.construct_tile_gen_sql(node, is_on_demand=False)
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
    window: int,
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
    window : int
        Window size

    Returns
    -------
    sql
    """
    num_tiles = int(window // frequency)
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
