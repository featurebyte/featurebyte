"""
Unit test for execution graph
"""
import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import Node, QueryGraph


@pytest.fixture(name="graph", scope="module")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()


@pytest.fixture(name="graph_single_node", scope="module")
def query_graph_single_node(graph):
    """
    Query graph with a single node
    """
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    assert graph.to_dict() == {
        "nodes": {
            "input_1": {
                "name": "input_1",
                "type": "input",
                "parameters": {},
                "output_type": "frame",
            }
        },
        "edges": {},
    }
    assert node_input == Node(name="input_1", type="input", parameters={}, output_type="frame")
    yield graph, node_input


@pytest.fixture(name="graph_two_nodes", scope="module")
def query_graph_two_nodes(graph_single_node):
    """
    Query graph with two nodes
    """
    graph, node_input = graph_single_node
    node_proj = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_proj == Node(
        name="project_1", type="project", parameters={"columns": ["a"]}, output_type="series"
    )
    yield graph, node_input, node_proj


@pytest.fixture(name="graph_three_nodes", scope="module")
def query_graph_three_nodes(graph_two_nodes):
    """
    Query graph with three nodes
    """
    graph, node_input, node_proj = graph_two_nodes
    node_eq = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
            "eq_1": {"type": "eq", "parameters": {"value": 1}, "output_type": "series"},
        },
        "edges": {
            "input_1": ["project_1"],
            "project_1": ["eq_1"],
        },
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_eq == Node(name="eq_1", type="eq", parameters={"value": 1}, output_type="series")
    yield graph, node_input, node_proj, node_eq


@pytest.fixture(name="graph_four_nodes", scope="module")
def query_graph_four_nodes(graph_three_nodes):
    """
    Query graph with four nodes
    """
    graph, node_input, node_proj, node_eq = graph_three_nodes
    node_filter = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, node_eq],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
            "eq_1": {"type": "eq", "parameters": {"value": 1}, "output_type": "series"},
            "filter_1": {"type": "filter", "parameters": {}, "output_type": "frame"},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["eq_1"],
            "eq_1": ["filter_1"],
        },
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_filter == Node(name="filter_1", type="filter", parameters={}, output_type="frame")
    yield graph, node_input, node_proj, node_eq, node_filter


def test_add_operation__add_duplicated_node_on_two_nodes_graph(graph_two_nodes):
    """
    Test add operation by adding a duplicated node on a 2-node graph
    """
    graph, node_input, node_proj = graph_two_nodes
    node_duplicated = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
        },
        "edges": {"input_1": ["project_1"]},
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_duplicated == node_proj


def test_add_operation__add_duplicated_node_on_four_nodes_graph(graph_four_nodes):
    """
    Test add operation by adding a duplicated node on a 4-node graph
    """
    graph, _, node_proj, node_eq, _ = graph_four_nodes
    node_duplicated = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_proj],
    )
    expected_graph = {
        "nodes": {
            "input_1": {"type": "input", "parameters": {}, "output_type": "frame"},
            "project_1": {
                "type": "project",
                "parameters": {"columns": ["a"]},
                "output_type": "series",
            },
            "eq_1": {"type": "eq", "parameters": {"value": 1}, "output_type": "series"},
            "filter_1": {"type": "filter", "parameters": {}, "output_type": "frame"},
        },
        "edges": {
            "input_1": ["project_1", "filter_1"],
            "project_1": ["eq_1"],
            "eq_1": ["filter_1"],
        },
    }
    assert graph.to_dict(exclude_name=True) == expected_graph
    assert node_duplicated == node_eq


from typing import Optional

from dataclasses import dataclass

import sqlglot
from sqlglot import Expression, expressions, parse_one, select


class SQLNode:
    @property
    def sql(self):
        raise NotImplementedError()


@dataclass
class TableNode(SQLNode):
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
    table: TableNode  # TODO: can also be AssignNode. FilterNode?
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
        sql_node = TableNode(
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
        tile_generating_nodes = []
        for node in self.g.nodes.values():
            if node["type"] in {"groupby"}:
                tile_generating_nodes.append(node)
        sqls = []
        for node in tile_generating_nodes:
            sql = self.tile_sql(node)
            import json
            tile_table_id = hash(json.dumps(node["parameters"]))  # TODO
            frequency = node["parameters"]["frequency"]
            blind_spot = node["parameters"]["blind_spot"]
            info = TileGenSql(
                sql=sql, tile_table_id=tile_table_id, frequency=frequency, blind_spot=blind_spot
            )
            sqls.append(info)
        return sqls

    def tile_sql(self, groupby_node):
        sql_nodes = {}
        groupby_sql_node = construct_sql_nodes(groupby_node, sql_nodes, self.g)
        res = groupby_sql_node.tile_sql()
        return res

    def construct_feature_from_tile_sql(self):
        raise NotImplementedError()

    def construct_feature_sql(self):
        raise NotImplementedError()


def test_graph_interpreter_super_simple(graph):
    query_graph = graph
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={"columns": ["ts", "cust_id", "a", "b"], "timestamp": "ts"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    assign = query_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "a_copy"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input, proj_a],
    )
    sql_nodes = {}
    construct_sql_nodes(query_graph.nodes[assign.name], sql_nodes, query_graph)
    sql_tree = sql_nodes["assign_1"].sql
    assert sql_tree.sql() == (
        "SELECT ts, cust_id, a, b, a AS a_copy FROM (SELECT ts, cust_id, a, b FROM event_table WHERE ts >= FBT_START_DATE AND ts < FBT_END_DATE)"
    )


def test_graph_interpreter_multi_assign(graph):
    query_graph = graph
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={"columns": ["ts", "cust_id", "a", "b"], "timestamp": "ts"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    sum_node = query_graph.add_operation(
        node_type=NodeType.ADD, node_params={}, node_output_type=NodeOutputType.SERIES, input_nodes=[proj_a, proj_b]
    )
    assign_node = query_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, sum_node],
    )
    proj_c = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["c"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[assign_node],
    )
    assign_node_2 = query_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c2"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, proj_c],
    )
    sql_nodes = {}
    construct_sql_nodes(query_graph.nodes[assign_node_2.name], sql_nodes, query_graph)
    sql_tree = sql_nodes["assign_2"].sql
    assert sql_tree.sql() == (
        "SELECT ts, cust_id, a, b, c, c AS c2 FROM (SELECT ts, cust_id, a, b, a + b AS c FROM (SELECT ts, cust_id, a, b FROM event_table WHERE ts >= FBT_START_DATE AND ts < FBT_END_DATE))"
    )


def test_graph_interpreter(graph):
    query_graph = graph
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={"columns": ["ts", "cust_id", "a", "b"], "timestamp": "ts"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = query_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    sum_node = query_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = query_graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, sum_node],
    )
    _groupby_node = query_graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            "key": "cust_id",
            "parent": "amount",
            "agg_func": "sum",
            "frequency": 30,
            "blind_spot": 1,
            "timestamp": "ts",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    interpreter = GraphInterpreter(query_graph)
    tile_gen_sql = interpreter.construct_tile_gen_sql()
    assert len(tile_gen_sql) == 1
    sql = tile_gen_sql[0].sql
    from sqlglot import transpile

    print()
    print(transpile(sql.sql(), pretty=True)[0])
