import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter, construct_sql_nodes


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()


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
