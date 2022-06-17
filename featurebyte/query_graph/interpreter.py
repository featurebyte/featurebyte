"""
This module contains the Query Graph Interpreter
"""
# pylint: disable=W0511
from typing import Any, Dict, List, Union

from dataclasses import dataclass
from enum import Enum

import sqlglot

from featurebyte.query_graph.enum import NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.sql import (
    BINARY_OPERATION_NODE_TYPES,
    AssignNode,
    BuildTileInputNode,
    ExpressionNode,
    GenericInputNode,
    SQLNode,
    TableNode,
    make_binary_operation_node,
    make_build_tile_node,
    make_filter_node,
    make_project_node,
)


class SQLType(Enum):
    """Type of SQL code corresponding to different operations"""

    BUILD_TILE = "build_tile"
    PREVIEW = "preview"


class SQLOperationGraph:
    """Construct a tree of SQL operations given a QueryGraph

    Parameters
    ----------
    query_graph : QueryGraph
        Query Graph representing user's intention
    sql_type : SQLType
        Type of SQL to generate
    """

    def __init__(self, query_graph: QueryGraph, sql_type: SQLType) -> None:
        self.sql_nodes: Dict[str, Union[SQLNode, TableNode]] = {}
        self.query_graph = query_graph
        self.sql_type = sql_type

    def build(self, target_node: Dict[str, Any]) -> Any:
        """Build the graph from a given query Node, working backwards

        Parameters
        ----------
        target_node : dict
            Dict representation of Query Graph Node. Build graph from this node backwards. This is
            typically the last node in the Query Graph, but can also be an intermediate node.

        Returns
        -------
        SQLNode
        """
        sql_node = self._construct_sql_nodes(target_node)
        return sql_node

    def get_node(self, name: str) -> SQLNode:
        """Get a node by name

        Parameters
        ----------
        name : str
            Node name

        Returns
        -------
        SQLNode
        """
        return self.sql_nodes[name]

    def _construct_sql_nodes(self, cur_node: Dict[str, Any]) -> Any:
        """Recursively construct the nodes

        Parameters
        ----------
        cur_node : dict
            Dictionary representation of Query Graph Node

        Returns
        -------
        SQLNode

        Raises
        ------
        NotImplementedError
            If a query node is not yet supported
        """
        cur_node_id = cur_node["name"]
        assert cur_node_id not in self.sql_nodes

        # Recursively build input sql nodes first
        inputs = self.query_graph.backward_edges[cur_node_id]
        input_sql_nodes = []
        for input_node_id in inputs:
            if input_node_id not in self.sql_nodes:
                input_node = self.query_graph.nodes[input_node_id]
                self._construct_sql_nodes(input_node)
            input_sql_node = self.sql_nodes[input_node_id]
            input_sql_nodes.append(input_sql_node)

        # Now that input sql nodes are ready, build the current sql node
        node_id = cur_node["name"]
        node_type = cur_node["type"]
        parameters = cur_node["parameters"]
        output_type = cur_node["output_type"]

        sql_node: Any
        if node_type == NodeType.INPUT:
            klass: Any
            if self.sql_type == SQLType.BUILD_TILE:
                klass = BuildTileInputNode
            else:
                klass = GenericInputNode
            sql_node = klass(
                column_names=parameters["columns"],
                timestamp=parameters["timestamp"],
                dbtable=parameters["dbtable"],
            )

        elif node_type == NodeType.ASSIGN:
            assert len(input_sql_nodes) == 2
            assert isinstance(input_sql_nodes[0], TableNode)
            sql_node = AssignNode(
                table_node=input_sql_nodes[0],
                column_node=input_sql_nodes[1],
                name=parameters["name"],
            )

        elif node_type == NodeType.PROJECT:
            sql_node = make_project_node(input_sql_nodes, parameters, output_type)

        elif node_type in BINARY_OPERATION_NODE_TYPES:
            sql_node = make_binary_operation_node(node_type, input_sql_nodes, parameters)

        elif node_type == NodeType.FILTER:
            sql_node = make_filter_node(input_sql_nodes, output_type)

        elif node_type == NodeType.GROUPBY:
            sql_node = make_build_tile_node(input_sql_nodes, parameters)

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
    """

    # TODO: tile_table_id likely should be determined by interpreter as well
    # tile_table_id: str
    sql: str
    columns: List[str]
    time_modulo_frequency: int
    frequency: int
    blind_spot: int


class TileSQLGenerator:
    """Generator for Tile-building SQL

    Parameters
    ----------
    query_graph : QueryGraph
    """

    def __init__(self, query_graph: QueryGraph):
        self.query_graph = query_graph

    def construct_tile_gen_sql(self) -> List[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        There can be more than one tile table to build if the feature depends on more than one
        groupby operations. However, before we support complex features, there will only be one tile
        table to build.

        Returns
        -------
        list[TileGenSql]
        """
        # Groupby operations requires building tiles (assuming the aggregation type supports tiling)
        tile_generating_nodes = []
        for node in self.query_graph.nodes.values():
            if node["type"] in {"groupby"}:
                tile_generating_nodes.append(node)

        sqls = []
        for node in tile_generating_nodes:
            info = self.make_one_tile_sql(node)
            sqls.append(info)

        return sqls

    def make_one_tile_sql(self, groupby_node: Dict[str, Any]) -> TileGenSql:
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: dict
            Dict representation of a groupby query graph node

        Returns
        -------
        TileGenSql
        """
        groupby_sql_node = SQLOperationGraph(
            query_graph=self.query_graph, sql_type=SQLType.BUILD_TILE
        ).build(groupby_node)
        sql = groupby_sql_node.sql
        frequency = groupby_node["parameters"]["frequency"]
        blind_spot = groupby_node["parameters"]["blind_spot"]
        time_modulo_frequency = groupby_node["parameters"]["time_modulo_frequency"]
        info = TileGenSql(
            sql=sql.sql(pretty=True),
            columns=groupby_sql_node.columns,
            time_modulo_frequency=time_modulo_frequency,
            frequency=frequency,
            blind_spot=blind_spot,
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

    def construct_tile_gen_sql(self) -> List[TileGenSql]:
        """Construct a list of tile building SQLs for the given Query Graph

        Returns
        -------
        List[TileGenSql]
        """
        generator = TileSQLGenerator(self.query_graph)
        return generator.construct_tile_gen_sql()

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
        sql_graph = SQLOperationGraph(self.query_graph, sql_type=SQLType.PREVIEW)
        sql_graph.build(self.query_graph.nodes[node_name])

        sql_node = sql_graph.get_node(node_name)
        assert isinstance(sql_node, (TableNode, ExpressionNode))
        if isinstance(sql_node, TableNode):
            sql_tree = sql_node.sql
        else:
            sql_tree = sql_node.sql_standalone

        assert isinstance(sql_tree, sqlglot.expressions.Select)
        sql_code: str = sql_tree.limit(num_rows).sql(pretty=True)

        return sql_code
