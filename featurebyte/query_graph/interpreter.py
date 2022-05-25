from typing import Optional, List

from dataclasses import dataclass
import json

import sqlglot
from sqlglot import Expression, expressions, parse_one, select
from .graph import Node, QueryGraph
from .enum import NodeType


class SQLNode:
    @property
    def sql(self):
        raise NotImplementedError()


@dataclass
class InputNode(SQLNode):
    columns: list[str]
    timestamp: str
    input: SQLNode

    @property
    def sql(self):
        s = select()
        for c in self.columns:
            s = s.select(c)
        if isinstance(self.input, ExpressionNode):
            s = s.from_(self.input.sql)
        else:
            s = s.from_(self.input.sql.subquery())
        # TODO: this is only for tile-gen sql
        s = s.where(f"{self.timestamp} >= FBT_START_DATE", f"{self.timestamp} < FBT_END_DATE")
        return s


@dataclass
class ExpressionNode(SQLNode):
    expr: sqlglot.Expression

    @property
    def sql(self):
        return self.expr


@dataclass
class SumNode(SQLNode):
    left: ExpressionNode
    right: ExpressionNode

    @property
    def sql(self):
        return parse_one(f"{self.left.sql.sql()} + {self.right.sql.sql()}")


@dataclass
class Project(SQLNode):
    columns: list[str]

    @property
    def sql(self):
        assert len(self.columns) == 1
        return parse_one(self.columns[0])


@dataclass
class AssignNode(SQLNode):
    table: InputNode  # TODO: can also be AssignNode. FilterNode?
    column: ExpressionNode
    name: str

    def __post_init__(self):
        self.columns = [x for x in self.table.columns if x not in self.name] + [self.name]

    @property
    def sql(self):
        s = select()
        for col in self.table.columns:
            if col == self.name:
                continue
            s = s.select(col)
        s = s.select(expressions.alias_(self.column.sql, self.name))
        s = s.from_(self.table.sql.subquery())
        return s


@dataclass
class GroupByNode:
    input: SQLNode
    key: str
    parent: str
    timestamp: str
    agg_func: str
    frequency: int
    blind_spot: int

    def tile_sql(self):
        start_date_placeholder = "FBT_START_DATE"
        input_tiled = select(
            "*", f"({self.timestamp} - {start_date_placeholder}) / {self.frequency} AS tile_index"
        ).from_(self.input.sql.subquery())
        groupby_sql = (
            select(
                f"(max(tile_index) + 1) * {self.frequency} + {start_date_placeholder} AS end_date",
                self.key,
                f"{self.agg_func}({self.parent}) AS value",
            )
                .from_(input_tiled.subquery())
                .group_by(f"tile_index", self.key)
        )
        return groupby_sql


@dataclass
class TileGenSql:
    tile_table_id: str
    sql: Expression
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
            tile_table_id = str(hash(json.dumps(node["parameters"])))  # TODO
            frequency = node["parameters"]["frequency"]
            blind_spot = node["parameters"]["blind_spot"]
            info = TileGenSql(
                sql=sql, tile_table_id=tile_table_id, frequency=frequency, blind_spot=blind_spot
            )
            sqls.append(info)
        return sqls

    def tile_sql(self, groupby_node: dict):
        sql_nodes = {}
        groupby_sql_node = construct_sql_nodes(groupby_node, sql_nodes, self.g)
        res = groupby_sql_node.tile_sql()
        return res


def construct_sql_nodes(cur_node, sql_nodes, g: QueryGraph):

    cur_node_id = cur_node["name"]
    assert cur_node_id not in sql_nodes

    inputs = g.backward_edges[cur_node_id]
    input_sql_nodes = []
    for input_node_id in inputs:
        if input_node_id not in sql_nodes:
            input_node = g.nodes[input_node_id]
            construct_sql_nodes(input_node, sql_nodes, g)
        input_sql_node = sql_nodes[input_node_id]
        input_sql_nodes.append(input_sql_node)

    node_id = cur_node["name"]
    node_type = cur_node["type"]
    parameters = cur_node["parameters"]

    if node_type == NodeType.INPUT:
        sql_node = InputNode(
            columns=parameters["columns"],
            timestamp=parameters["timestamp"],
            input=ExpressionNode(parse_one("event_table")),
        )

    elif node_type == NodeType.ASSIGN:
        assert len(input_sql_nodes) == 2
        sql_node = AssignNode(
            table=input_sql_nodes[0], column=input_sql_nodes[1], name=parameters["name"]
        )

    elif node_type == NodeType.PROJECT:
        sql_node = Project(parameters["columns"])

    elif node_type == NodeType.ADD:
        sql_node = SumNode(input_sql_nodes[0], input_sql_nodes[1])

    elif node_type == NodeType.GROUPBY:
        sql_node = GroupByNode(
            input=input_sql_nodes[0],
            key=parameters["key"],
            parent=parameters["parent"],
            timestamp=parameters["timestamp"],
            agg_func=parameters["agg_func"],
            frequency=parameters["frequency"],
            blind_spot=parameters["blind_spot"],
        )

    else:
        raise NotImplementedError(f"SQLNode not implemented for {cur_node}")

    sql_nodes[node_id] = sql_node
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
