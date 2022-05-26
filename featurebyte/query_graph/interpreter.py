"""
This module contains the Query Graph Interpreter
"""
# pylint: disable=W0511
from typing import List

from dataclasses import dataclass

from .enum import NodeType
from .graph import QueryGraph
from .sql import AddNode, AssignNode, BuildTileNode, ExpressionNode, InputNode, Project


class SQLOperationGraph:
    """Construct a tree of SQL operations given a QueryGraph

    Parameters
    ----------
    query_graph : QueryGraph
        Query Graph representing user's intention
    """

    def __init__(self, query_graph: QueryGraph):
        self.sql_nodes = {}
        self.query_graph = query_graph

    def build(self, starting_node: dict):
        """Build the graph from a given query Node, working backwards

        Parameters
        ----------
        starting_node : dict
            Dict representation of Query Graph Node. Build graph from this node backwards. This is
            typically the last node in the Query Graph, but can also be an intermediate node.
        """
        sql_node = self._construct_sql_nodes(starting_node)
        return sql_node

    def get_node(self, name: str):
        """Get a node by name

        Parameters
        ----------
        name : str
            Node name
        """
        return self.sql_nodes[name]

    def _construct_sql_nodes(self, cur_node):
        """Recursively construct the nodes

        Parameters
        ----------
        cur_node : dict
            Dictionary representation of Query Graph Node
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

        if node_type == NodeType.INPUT:
            sql_node = InputNode(
                columns=parameters["columns"],
                timestamp=parameters["timestamp"],
                input=ExpressionNode(parameters["dbtable"]),
            )

        elif node_type == NodeType.ASSIGN:
            assert len(input_sql_nodes) == 2
            sql_node = AssignNode(
                table=input_sql_nodes[0], column=input_sql_nodes[1], name=parameters["name"]
            )

        elif node_type == NodeType.PROJECT:
            sql_node = Project(parameters["columns"])

        elif node_type == NodeType.ADD:
            sql_node = AddNode(input_sql_nodes[0], input_sql_nodes[1])

        elif node_type == NodeType.GROUPBY:
            sql_node = BuildTileNode(
                input=input_sql_nodes[0],
                key=parameters["key"],
                parent=parameters["parent"],
                timestamp=parameters["timestamp"],
                agg_func=parameters["agg_func"],
                frequency=parameters["frequency"],
            )

        else:
            raise NotImplementedError(f"SQLNode not implemented for {cur_node}")

        self.sql_nodes[node_id] = sql_node
        return sql_node


@dataclass
class TileGenSql:
    """Information about a tile building SQL"""

    # TODO: tile_table_id likely should be determined by interpreter as well
    # tile_table_id: str
    sql: str
    columns: List[str]
    window_end: int
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

    def make_one_tile_sql(self, groupby_node: dict):
        """Construct tile building SQL for a specific groupby query graph node

        Parameters
        ----------
        groupby_node: dict
            Dict representation of a groupby query graph node
        """
        groupby_sql_node = SQLOperationGraph(self.query_graph).build(groupby_node)
        sql = groupby_sql_node.sql
        frequency = groupby_node["parameters"]["frequency"]
        blind_spot = groupby_node["parameters"]["blind_spot"]
        window_end = groupby_node["parameters"]["window_end"]
        info = TileGenSql(
            sql=sql.sql(pretty=True),
            columns=groupby_sql_node.columns,
            window_end=window_end,
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

    def construct_feature_from_tile_sql(self):
        """Construct SQL that computes feature from tile table"""
        raise NotImplementedError()

    def construct_feature_brute_force_sql(self):
        """Construct SQL that computes feature without using tiling optimization"""
        raise NotImplementedError()
