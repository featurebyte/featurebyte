"""
Module for tile related sql generation
"""
from __future__ import annotations

from typing import Any

from dataclasses import dataclass

from sqlglot import Expression, expressions, select

from featurebyte.enum import InternalName
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.sql.ast.base import SQLNode, TableNode
from featurebyte.query_graph.sql.common import AggregationSpec, SQLType, quoted_identifier
from featurebyte.query_graph.sql.tiling import TileSpec, get_aggregator


@dataclass
class BuildTileNode(TableNode):
    """Tile builder node

    This node is responsible for generating the tile building SQL for a groupby operation.
    """

    input_node: TableNode
    keys: list[str]
    value_by: str | None
    tile_specs: list[TileSpec]
    timestamp: str
    agg_func: str
    frequency: int
    is_on_demand: bool

    @property
    def sql(self) -> Expression:
        if self.is_on_demand:
            start_date_expr = InternalName.ENTITY_TABLE_START_DATE
        else:
            start_date_expr = InternalName.TILE_START_DATE_SQL_PLACEHOLDER

        start_date_epoch = f"DATE_PART(EPOCH_SECOND, CAST({start_date_expr} AS TIMESTAMP))"
        timestamp_epoch = f"DATE_PART(EPOCH_SECOND, {quoted_identifier(self.timestamp).sql()})"

        input_tiled = select(
            "*",
            f"FLOOR(({timestamp_epoch} - {start_date_epoch}) / {self.frequency}) AS tile_index",
        ).from_(self.input_node.sql_nested())

        tile_start_date = f"TO_TIMESTAMP({start_date_epoch} + tile_index * {self.frequency})"
        keys = [quoted_identifier(k) for k in self.keys]
        if self.value_by is not None:
            keys.append(quoted_identifier(self.value_by))

        if self.is_on_demand:
            groupby_keys = keys + [InternalName.ENTITY_TABLE_START_DATE.value]
        else:
            groupby_keys = keys

        groupby_sql = (
            select(
                f"{tile_start_date} AS {InternalName.TILE_START_DATE}",
                *keys,
                *[f"{spec.tile_expr} AS {spec.tile_column_name}" for spec in self.tile_specs],
            )
            .from_(input_tiled.subquery())
            .group_by("tile_index", *groupby_keys)
            .order_by("tile_index")
        )

        return groupby_sql


@dataclass
class AggregatedTilesNode(TableNode):
    """Node with tiles already aggregated

    The purpose of this node is to allow feature SQL generation to retrieve the post-aggregation
    feature transform expression. The columns_map of this node has the mapping from user defined
    feature names to internal aggregated column names. The feature expression can be obtained by
    calling get_column_expr().
    """

    @property
    def sql(self) -> Expression:
        # This will not be called anywhere
        raise NotImplementedError()


def make_build_tile_node(
    input_sql_nodes: list[SQLNode], parameters: dict[str, Any], is_on_demand: bool
) -> BuildTileNode:
    """Create a BuildTileNode

    Parameters
    ----------
    input_sql_nodes : list[SQLNode]
        List of input SQL nodes
    parameters : dict[str, Any]
        Query node parameters
    is_on_demand : bool
        Whether the SQL is for on-demand tile building for historical features

    Returns
    -------
    BuildTileNode
    """
    input_node = input_sql_nodes[0]
    assert isinstance(input_node, TableNode)
    aggregator = get_aggregator(parameters["agg_func"])
    tile_specs = aggregator.tile(parameters["parent"], parameters["aggregation_id"])
    columns = (
        [InternalName.TILE_START_DATE.value]
        + parameters["keys"]
        + [spec.tile_column_name for spec in tile_specs]
    )
    columns_map = {col: expressions.Identifier(this=col, quoted=True) for col in columns}
    sql_node = BuildTileNode(
        columns_map=columns_map,
        input_node=input_node,
        keys=parameters["keys"],
        value_by=parameters["value_by"],
        tile_specs=tile_specs,
        timestamp=parameters["timestamp"],
        agg_func=parameters["agg_func"],
        frequency=parameters["frequency"],
        is_on_demand=is_on_demand,
    )
    return sql_node


def make_aggregated_tiles_node(groupby_node: Node) -> AggregatedTilesNode:
    """Create a TableNode representing the aggregated tiles

    Parameters
    ----------
    groupby_node : Node
        Query graph node with groupby type

    Returns
    -------
    AggregatedTilesNode
    """
    agg_specs = AggregationSpec.from_groupby_query_node(groupby_node)
    columns_map = {}
    for agg_spec in agg_specs:
        columns_map[agg_spec.feature_name] = expressions.Identifier(
            this=agg_spec.agg_result_name, quoted=True
        )
    return AggregatedTilesNode(columns_map=columns_map)


def handle_groupby_node(
    groupby_node: Node,
    parameters: dict[str, Any],
    input_sql_nodes: list[SQLNode],
    sql_type: SQLType,
) -> BuildTileNode | AggregatedTilesNode:
    """Handle a groupby query graph node and create an appropriate SQLNode

    Parameters
    ----------
    groupby_node : Node
        Groupby query graph
    parameters : dict[str, Any]
        Query node parameters
    input_sql_nodes : list[SQLNode]
        Input SQL nodes
    sql_type : SQLType
        Type of SQL code to generate

    Returns
    -------
    BuildTileNode | AggregatedTilesNode
        Resulting SQLNode

    Raises
    ------
    NotImplementedError
        If the provided query node is not supported
    """
    sql_node: BuildTileNode | AggregatedTilesNode
    if sql_type == SQLType.BUILD_TILE:
        sql_node = make_build_tile_node(input_sql_nodes, parameters, is_on_demand=False)
    elif sql_type == SQLType.BUILD_TILE_ON_DEMAND:
        sql_node = make_build_tile_node(input_sql_nodes, parameters, is_on_demand=True)
    elif sql_type == SQLType.GENERATE_FEATURE:
        sql_node = make_aggregated_tiles_node(groupby_node)
    else:
        raise NotImplementedError(f"SQLNode not implemented for {groupby_node}")
    return sql_node
