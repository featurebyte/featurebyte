"""
This module contains the Query Graph Interpreter
"""
from __future__ import annotations

from typing import cast

from dataclasses import dataclass

import sqlglot

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.model import QueryGraphModel
from featurebyte.query_graph.node import Node
from featurebyte.query_graph.node.generic import GroupbyNode
from featurebyte.query_graph.sql.ast.base import ExpressionNode, TableNode
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType, sql_to_string
from featurebyte.query_graph.sql.template import SqlExpressionTemplate


@dataclass
class TileGenSql:
    """Information about a tile building SQL

    This information is required by the Tile Manager to perform tile related operations such as
    scheduling tile computation jobs.

    Parameters
    ----------
    sql : str
        Templated SQL code for building tiles
    columns : List[str]
        List of columns in the tile table after executing the SQL code
    time_modulo_frequency: int
        Offset used to determine the time for jobs scheduling. Should be smaller than frequency.
    frequency : int
        Job frequency. Needed for job scheduling.
    blind_spot : int
        Blind spot. Needed for job scheduling.
    windows : list[int]
        List of window sizes. Not needed for job scheduling, but can be used for other purposes such
        as determining the reuqired tiles to build on demand during preview.
    """

    # pylint: disable=too-many-instance-attributes
    tile_table_id: str
    aggregation_id: str
    sql_template: SqlExpressionTemplate
    columns: list[str]
    entity_columns: list[str]
    serving_names: list[str]
    value_by_column: str | None
    tile_value_columns: list[str]
    time_modulo_frequency: int
    frequency: int
    blind_spot: int
    windows: list[str]

    @property
    def sql(self) -> str:
        """
        Templated SQL code for building tiles

        Returns
        -------
        str
        """
        return cast(str, self.sql_template.render())


class TileSQLGenerator:
    """Generator for Tile-building SQL

    Parameters
    ----------
    query_graph : QueryGraphModel
    """

    def __init__(self, query_graph: QueryGraphModel, is_on_demand: bool, source_type: SourceType):
        self.query_graph = query_graph
        self.is_on_demand = is_on_demand
        self.source_type = source_type

    def construct_tile_gen_sql(self, starting_node: Node) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        There can be more than one tile table to build if the feature depends on more than one
        groupby operations. However, before we support complex features, there will only be one tile
        table to build.

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from

        Returns
        -------
        list[TileGenSql]
        """
        # Groupby operations requires building tiles (assuming the aggregation type supports tiling)
        tile_generating_nodes = {}
        for node in self.query_graph.iterate_nodes(starting_node, NodeType.GROUPBY):
            assert isinstance(node, GroupbyNode)
            tile_generating_nodes[node.name] = node

        sqls = []
        for node in tile_generating_nodes.values():
            info = self.make_one_tile_sql(node)
            sqls.append(info)

        return sqls

    def make_one_tile_sql(self, groupby_node: GroupbyNode) -> TileGenSql:
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: GroupbyNode
            Groupby query graph node

        Returns
        -------
        TileGenSql
        """
        if self.is_on_demand:
            sql_type = SQLType.BUILD_TILE_ON_DEMAND
        else:
            sql_type = SQLType.BUILD_TILE
        groupby_sql_node = SQLOperationGraph(
            query_graph=self.query_graph, sql_type=sql_type, source_type=self.source_type
        ).build(groupby_node)
        sql = groupby_sql_node.sql
        tile_table_id = groupby_node.parameters.tile_id
        aggregation_id = groupby_node.parameters.aggregation_id
        entity_columns = groupby_sql_node.keys
        tile_value_columns = [spec.tile_column_name for spec in groupby_sql_node.tile_specs]
        assert tile_table_id is not None
        assert aggregation_id is not None
        sql_template = SqlExpressionTemplate(sql_expr=sql, source_type=self.source_type)
        info = TileGenSql(
            tile_table_id=tile_table_id,
            aggregation_id=aggregation_id,
            sql_template=sql_template,
            columns=groupby_sql_node.columns,
            entity_columns=entity_columns,
            tile_value_columns=tile_value_columns,
            time_modulo_frequency=groupby_node.parameters.time_modulo_frequency,
            frequency=groupby_node.parameters.frequency,
            blind_spot=groupby_node.parameters.blind_spot,
            windows=groupby_node.parameters.windows,
            serving_names=groupby_node.parameters.serving_names,
            value_by_column=groupby_node.parameters.value_by,
        )
        return info


class GraphInterpreter:
    """Interprets a given Query Graph and generates SQL for different purposes

    Parameters
    ----------
    query_graph : QueryGraphModel
        Query graph
    source_type : SourceType
        Data source type information
    """

    def __init__(self, query_graph: QueryGraphModel, source_type: SourceType):
        self.query_graph = query_graph
        self.source_type = source_type

    def construct_tile_gen_sql(self, starting_node: Node, is_on_demand: bool) -> list[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        Parameters
        ----------
        starting_node : Node
            Starting node (typically corresponding to selected features) to search from
        is_on_demand : bool
            Whether the SQL is for on-demand tile building for historical features

        Returns
        -------
        List[TileGenSql]
        """
        generator = TileSQLGenerator(
            self.query_graph, is_on_demand=is_on_demand, source_type=self.source_type
        )
        return generator.construct_tile_gen_sql(starting_node)

    def construct_preview_sql(self, node_name: str, num_rows: int = 10) -> str:
        """Construct SQL to preview a given node

        Parameters
        ----------
        node_name : str
            Query graph node name
        num_rows : int
            Number of rows to include in the preview

        Returns
        -------
        str
            SQL code for preview purpose
        """
        sql_graph = SQLOperationGraph(
            self.query_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=self.source_type
        )
        sql_node = sql_graph.build(self.query_graph.get_node_by_name(node_name))

        assert isinstance(sql_node, (TableNode, ExpressionNode))
        if isinstance(sql_node, TableNode):
            sql_tree = sql_node.sql
        else:
            sql_tree = sql_node.sql_standalone

        assert isinstance(sql_tree, sqlglot.expressions.Select)
        sql_code: str = sql_to_string(sql_tree.limit(num_rows), source_type=self.source_type)

        return sql_code
