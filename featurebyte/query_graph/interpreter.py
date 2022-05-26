from dataclasses import dataclass
import json

from .graph import QueryGraph
from .enum import NodeType
from .sql import *


@dataclass
class TileGenSql:
    tile_table_id: str
    sql: str
    # columns: List[str]  # TODO
    window_end: int
    frequency: int
    blind_spot: int


class TileSQLGenerator:

    def __init__(self, g: QueryGraph):
        self.g = g

    def construct_tile_gen_sql(self) -> list[TileGenSql]:
        tile_generating_nodes = []
        for node in self.g.nodes.values():
            if node["type"] in {"groupby"}:
                tile_generating_nodes.append(node)
        sqls = []
        for node in tile_generating_nodes:
            sql = self.tile_sql(node)
            tile_table_id = str(abs(hash(json.dumps(node["parameters"]))))  # TODO
            frequency = node["parameters"]["frequency"]
            blind_spot = node["parameters"]["blind_spot"]
            window_end = node["parameters"]["window_end"]
            info = TileGenSql(
                sql=sql.sql(pretty=True),
                tile_table_id=tile_table_id,
                window_end=window_end,
                frequency=frequency,
                blind_spot=blind_spot,
            )
            sqls.append(info)
        return sqls

    def tile_sql(self, groupby_node: dict):
        groupby_sql_node = SQLOperationGraph(self.g).build(groupby_node)
        res = groupby_sql_node.tile_sql()
        return res


class SQLOperationGraph:

    def __init__(self, g: QueryGraph):
        self.sql_nodes = {}
        self.g = g

    def build(self, starting_node):
        sql_node = self._construct_sql_nodes(starting_node)
        return sql_node

    def get_node(self, name):
        return self.sql_nodes[name]

    def _construct_sql_nodes(self, cur_node):

        cur_node_id = cur_node["name"]
        assert cur_node_id not in self.sql_nodes

        # Recursively build input sql nodes first
        inputs = self.g.backward_edges[cur_node_id]
        input_sql_nodes = []
        for input_node_id in inputs:
            if input_node_id not in self.sql_nodes:
                input_node = self.g.nodes[input_node_id]
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


class GraphInterpreter:
    def __init__(self, g: QueryGraph):
        self.g = g

    def construct_tile_gen_sql(self) -> list[TileGenSql]:
        generator = TileSQLGenerator(self.g)
        return generator.construct_tile_gen_sql()

    def construct_feature_from_tile_sql(self):
        raise NotImplementedError()

    def construct_feature_sql(self):
        raise NotImplementedError()
