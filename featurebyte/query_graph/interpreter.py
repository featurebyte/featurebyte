"""
This module contains the Query Graph Interpreter
"""
from __future__ import annotations

from typing import Any, Iterator

from dataclasses import dataclass

import sqlglot

from featurebyte.query_graph.algorithm import dfs_traversal
from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import Node, QueryGraph
from featurebyte.query_graph.sql import (
    BINARY_OPERATION_NODE_TYPES,
    AliasNode,
    ExpressionNode,
    SQLNode,
    SQLType,
    TableNode,
    handle_groupby_node,
    make_binary_operation_node,
    make_conditional_node,
    make_filter_node,
    make_input_node,
    make_project_node,
)


class SQLOperationGraph:
    """Construct a tree of SQL operations given a QueryGraph

    Parameters
    ----------
    query_graph : QueryGraph
        Query Graph representing user's intention
    sql_type : SQLType
        Type of SQL to generate
    """

    # pylint: disable=too-few-public-methods

    def __init__(self, query_graph: QueryGraph, sql_type: SQLType) -> None:
        self.sql_nodes: dict[str, SQLNode | TableNode] = {}
        self.query_graph = query_graph
        self.sql_type = sql_type

    def build(self, target_node: Node) -> Any:
        """Build the graph from a given query Node, working backwards

        Parameters
        ----------
        target_node : Node
            Dict representation of Query Graph Node. Build graph from this node backwards. This is
            typically the last node in the Query Graph, but can also be an intermediate node.

        Returns
        -------
        SQLNode
        """
        if target_node.type == NodeType.GROUPBY:
            groupby_keys = target_node.parameters["keys"]
        else:
            groupby_keys = None
        sql_node = self._construct_sql_nodes(target_node, groupby_keys=groupby_keys)
        return sql_node

    def _construct_sql_nodes(self, cur_node: Node, groupby_keys: list[str] | None) -> Any:
        """Recursively construct the nodes

        Parameters
        ----------
        cur_node : Node
            Dictionary representation of Query Graph Node
        groupby_keys : list[str] | None
            Groupby keys in the current context. Used when sql_type is BUILD_TILE_ON_DEMAND

        Returns
        -------
        SQLNode

        Raises
        ------
        NotImplementedError
            If a query node is not yet supported
        """
        # pylint: disable=too-many-locals
        cur_node_id = cur_node.name
        assert cur_node_id not in self.sql_nodes

        # Recursively build input sql nodes first
        inputs = self.query_graph.backward_edges[cur_node_id]
        input_sql_nodes = []
        for input_node_id in inputs:
            if input_node_id not in self.sql_nodes:
                input_node = self.query_graph.get_node_by_name(input_node_id)
                # Note: In the lineage leading to any features or intermediate outputs, there can be
                # only one groupby operation (there can be parallel groupby operations, but not
                # consecutive ones)
                if groupby_keys is None and input_node.type == NodeType.GROUPBY:
                    groupby_keys = input_node.parameters["keys"]
                self._construct_sql_nodes(input_node, groupby_keys=groupby_keys)
            input_sql_node = self.sql_nodes[input_node_id]
            input_sql_nodes.append(input_sql_node)

        # Now that input sql nodes are ready, build the current sql node
        node_id = cur_node.name
        node_type = cur_node.type
        parameters = cur_node.parameters
        output_type = cur_node.output_type

        sql_node: Any
        if node_type == NodeType.INPUT:
            sql_node = make_input_node(parameters, self.sql_type, groupby_keys)

        elif node_type == NodeType.ASSIGN:
            assert len(input_sql_nodes) == 2
            assert isinstance(input_sql_nodes[0], TableNode)
            input_table_node = input_sql_nodes[0]
            input_table_node.set_column_expr(parameters["name"], input_sql_nodes[1].sql)
            sql_node = input_table_node

        elif node_type == NodeType.PROJECT:
            sql_node = make_project_node(input_sql_nodes, parameters, output_type)

        elif node_type == NodeType.ALIAS:
            expr_node = input_sql_nodes[0]
            assert isinstance(expr_node, ExpressionNode)
            sql_node = AliasNode(
                table_node=expr_node.table_node, name=parameters["name"], expr_node=expr_node
            )

        elif node_type in BINARY_OPERATION_NODE_TYPES:
            sql_node = make_binary_operation_node(node_type, input_sql_nodes, parameters)

        elif node_type == NodeType.FILTER:
            sql_node = make_filter_node(input_sql_nodes, output_type)

        elif node_type == NodeType.CONDITIONAL:
            sql_node = make_conditional_node(input_sql_nodes, cur_node)

        elif node_type == NodeType.GROUPBY:
            sql_node = handle_groupby_node(
                groupby_node=cur_node,
                parameters=parameters,
                input_sql_nodes=input_sql_nodes,
                sql_type=self.sql_type,
            )
        else:
            raise NotImplementedError(f"SQLNode not implemented for {cur_node}")

        self.sql_nodes[node_id] = sql_node
        return sql_node


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
    sql: str
    columns: list[str]
    entity_columns: list[str]
    serving_names: list[str]
    value_by_column: str | None
    tile_value_columns: list[str]
    time_modulo_frequency: int
    frequency: int
    blind_spot: int
    windows: list[int]


def find_parent_groupby_nodes(query_graph: QueryGraph, starting_node: Node) -> Iterator[Node]:
    """Helper function to find all groupby nodes in a Query Graph

    Parameters
    ----------
    query_graph : QueryGraph
        Query graph
    starting_node : Node
        Node from which to start the search

    Yields
    ------
    Node
        Query graph nodes of groupby type
    """
    for node in dfs_traversal(query_graph, starting_node):
        if node.type == NodeType.GROUPBY:
            yield node


class TileSQLGenerator:
    """Generator for Tile-building SQL

    Parameters
    ----------
    query_graph : QueryGraph
    """

    def __init__(self, query_graph: QueryGraph, is_on_demand: bool):
        self.query_graph = query_graph
        self.is_on_demand = is_on_demand

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
        for node in find_parent_groupby_nodes(self.query_graph, starting_node):
            tile_generating_nodes[node.name] = node

        sqls = []
        for node in tile_generating_nodes.values():
            info = self.make_one_tile_sql(node)
            sqls.append(info)

        return sqls

    def make_one_tile_sql(self, groupby_node: Node) -> TileGenSql:
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: Node
            Groupby query graph node

        Returns
        -------
        TileGenSql
        """
        if self.is_on_demand:
            sql_type = SQLType.BUILD_TILE_ON_DEMAND
        else:
            sql_type = SQLType.BUILD_TILE
        groupby_sql_node = SQLOperationGraph(query_graph=self.query_graph, sql_type=sql_type).build(
            groupby_node
        )
        sql = groupby_sql_node.sql
        frequency = groupby_node.parameters["frequency"]
        blind_spot = groupby_node.parameters["blind_spot"]
        time_modulo_frequency = groupby_node.parameters["time_modulo_frequency"]
        windows = groupby_node.parameters["windows"]
        tile_table_id = groupby_node.parameters["tile_id"]
        entity_columns = groupby_sql_node.keys
        serving_names = groupby_node.parameters["serving_names"]
        value_by_column = groupby_node.parameters["value_by"]
        tile_value_columns = [spec.tile_column_name for spec in groupby_sql_node.tile_specs]
        info = TileGenSql(
            tile_table_id=tile_table_id,
            sql=sql.sql(pretty=True),
            columns=groupby_sql_node.columns,
            entity_columns=entity_columns,
            tile_value_columns=tile_value_columns,
            time_modulo_frequency=time_modulo_frequency,
            frequency=frequency,
            blind_spot=blind_spot,
            windows=windows,
            serving_names=serving_names,
            value_by_column=value_by_column,
        )
        return info


class GraphInterpreter:
    """Interprets a given Query Graph and generates SQL for different purposes

    Parameters
    ----------
    query_graph : QueryGraph
    """

    def __init__(self, query_graph: QueryGraph):
        self.query_graph = query_graph

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
        generator = TileSQLGenerator(self.query_graph, is_on_demand=is_on_demand)
        return generator.construct_tile_gen_sql(starting_node)

    def construct_feature_from_tile_sql(self) -> None:
        """Construct SQL that computes feature from tile table

        Raises
        ------
        NotImplementedError
            Not implemented yet
        """
        raise NotImplementedError()

    def construct_feature_brute_force_sql(self) -> None:
        """Construct SQL that computes feature without using tiling optimization

        Raises
        ------
        NotImplementedError
            Not implemented yet
        """
        raise NotImplementedError()

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
        sql_graph = SQLOperationGraph(self.query_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW)
        sql_node = sql_graph.build(self.query_graph.get_node_by_name(node_name))

        assert isinstance(sql_node, (TableNode, ExpressionNode))
        if isinstance(sql_node, TableNode):
            sql_tree = sql_node.sql
        else:
            sql_tree = sql_node.sql_standalone

        assert isinstance(sql_tree, sqlglot.expressions.Select)
        sql_code: str = sql_tree.limit(num_rows).sql(pretty=True)

        return sql_code
