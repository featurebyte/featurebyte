"""
On-demand tile computation for feature preview
"""

from __future__ import annotations

from typing import Optional

import pandas as pd
from sqlglot import expressions
from sqlglot.expressions import Select, select

from featurebyte.enum import InternalName
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.adapter import BaseAdapter, get_sql_adapter
from featurebyte.query_graph.sql.aggregator.base import CommonTable
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.interpreter import GraphInterpreter, TileGenSql
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.tile_util import (
    construct_entity_table_query_for_window,
    get_max_window_sizes,
)


class OnDemandTileComputePlan:
    """Responsible for generating SQL to compute tiles for preview purpose

    Feature preview uses the same SQL query as historical feature requests. As a result, we need to
    build temporary tile tables that are required by the feature query. Actual tile tables are wide
    and consist of tile values from different transforms (aggregation_id). Based on the current
    implementation, for feature preview each groupby node has its own tile SQLs, so we need to
    perform some manipulation to construct the wide tile tables.

    Parameters
    ----------
    request_table_name : str
        Name of request table to use
    source_info: SourceInfo
        Source information
    """

    def __init__(
        self,
        request_table_name: str,
        source_info: SourceInfo,
    ):
        self.processed_agg_ids: set[str] = set()
        self.tile_infos: list[TileGenSql] = []
        self.all_tile_infos: list[TileGenSql] = []
        self.request_table_name = request_table_name
        self.source_info = source_info

    @property
    def adapter(self) -> BaseAdapter:
        """
        Returns an instance of BaseAdapter based on the source type

        Returns
        -------
        BaseAdapter
        """
        return get_sql_adapter(self.source_info)

    def process_node(self, graph: QueryGraphModel, node: Node) -> None:
        """Update state given a query graph node

        Parameters
        ----------
        graph : QueryGraphModel
            Query graph
        node : Node
            Query graph node
        """
        tile_gen_info_lst = get_tile_gen_info(graph, node, self.source_info)

        for tile_info in tile_gen_info_lst:
            self.all_tile_infos.append(tile_info)

            if tile_info.aggregation_id in self.processed_agg_ids:
                # The same aggregation_id can appear more than once. For example, two groupby
                # operations with the same parameters except windows will have the same
                # aggregation_id.
                continue

            self.tile_infos.append(tile_info)
            self.processed_agg_ids.add(tile_info.aggregation_id)

    def construct_tile_sqls(self) -> dict[str, Select]:
        """Construct SQL expressions for all the required tile tables

        Returns
        -------
        dict[str, expressions.Expression]
        """

        tile_sqls: dict[str, Select] = {}
        prev_aliases: dict[str, str] = {}

        # The date range of each tile table depends on the feature window sizes.
        max_window_size_by_tile_id = get_max_window_sizes(
            tile_info_list=self.all_tile_infos,
            key_name="tile_table_id",
        )

        for tile_info in self.tile_infos:
            # Construct tile SQL using an entity table (a table with entity column(s) as the primary
            # key representing the entities of interest) created from the request table and feature
            # window sizes
            tile_sql_expr = get_tile_sql(
                adapter=self.adapter,
                tile_info=tile_info,
                request_table_name=self.request_table_name,
                window=max_window_size_by_tile_id[tile_info.tile_table_id],
            )

            # TODO: simplify the logic below since tile table is no longer wide
            # Build wide tile table by joining tile sqls with the same tile_table_id
            tile_table_id = tile_info.tile_table_id
            agg_id = tile_info.aggregation_id
            assert isinstance(tile_sql_expr, expressions.Query)

            if tile_table_id not in tile_sqls:
                # New tile table - get the tile index column, entity columns and tile value columns
                keys = [
                    f"{agg_id}.{quoted_identifier(key).sql()}" for key in tile_info.entity_columns
                ]
                if tile_info.value_by_column is not None:
                    keys.append(f"{agg_id}.{quoted_identifier(tile_info.value_by_column).sql()}")
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
                    key = quoted_identifier(key).sql()
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

    def construct_on_demand_tile_ctes(self) -> list[CommonTable]:
        """Construct the CTE statements that would compute all the required tiles

        Returns
        -------
        list[tuple[str, str]]
        """
        cte_statements = []
        tile_sqls = self.construct_tile_sqls()
        for tile_table_id, tile_sql_expr in tile_sqls.items():
            cte_statements.append(CommonTable(tile_table_id, tile_sql_expr, quoted=False))
        return cte_statements


def get_tile_gen_info(
    graph: QueryGraphModel, node: Node, source_info: SourceInfo
) -> list[TileGenSql]:
    """Construct TileGenSql that contains recipe of building tiles

    Parameters
    ----------
    graph : QueryGraphModel
        Query graph
    node : Node
        Query graph node
    source_info: SourceInfo
        Source information

    Returns
    -------
    list[TileGenSql]
    """
    interpreter = GraphInterpreter(graph, source_info=source_info)
    tile_gen_info = interpreter.construct_tile_gen_sql(node, is_on_demand=True)
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


def get_tile_sql(
    adapter: BaseAdapter,
    tile_info: TileGenSql,
    request_table_name: str,
    window: Optional[int],
) -> Select:
    """
    Construct the SQL query that would compute the tiles for a given TileGenSql.

    TileGenSql already contains the template for the tile SQL. This function fills in the entity
    table placeholder so that the SQL is complete.

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
    Select
    """
    entity_table_expr = construct_entity_table_query_for_window(
        adapter=adapter,
        tile_info=tile_info,
        request_table_name=request_table_name,
        window=window,
    )
    return tile_info.tile_compute_query.replace_prerequisite_table_expr(
        name=InternalName.ENTITY_TABLE_NAME, new_expr=entity_table_expr
    ).get_combined_query_expr()
