import pytest
import textwrap

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
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
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
    expected = textwrap.dedent(
        """
        SELECT
          ts,
          cust_id,
          a,
          b,
          a AS a_copy
        FROM (
            SELECT
              ts,
              cust_id,
              a,
              b
            FROM event_table
            WHERE
              ts >= CAST(FBT_START_DATE AS TIMESTAMP)
              AND ts < CAST(FBT_END_DATE AS TIMESTAMP)
        )
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_graph_interpreter_multi_assign(graph):
    query_graph = graph
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
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
    expected = textwrap.dedent(
        """
        SELECT
          ts,
          cust_id,
          a,
          b,
          c,
          c AS c2
        FROM (
            SELECT
              ts,
              cust_id,
              a,
              b,
              a + b AS c
            FROM (
                SELECT
                  ts,
                  cust_id,
                  a,
                  b
                FROM event_table
                WHERE
                  ts >= CAST(FBT_START_DATE AS TIMESTAMP)
                  AND ts < CAST(FBT_END_DATE AS TIMESTAMP)
            )
        )
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_graph_interpreter_tile_gen(graph):
    query_graph = graph
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
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


def test_graph_interpreter_snowflake(graph):
    query_graph = graph
    node_input = query_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["SERVER_TIMESTAMP", "CUST_ID"],
            "timestamp": "SERVER_TIMESTAMP",
            "dbtable": '"FB_SIMULATE"."PUBLIC"."BROWSING_TS"',
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    _groupby_node = query_graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            "key": "CUST_ID",
            "parent": "*",
            "agg_func": "COUNT",
            "frequency": 3600,
            "blind_spot": 1,
            "timestamp": "SERVER_TIMESTAMP",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    interpreter = GraphInterpreter(query_graph)
    tile_gen_sql = interpreter.construct_tile_gen_sql()
    assert len(tile_gen_sql) == 1
    sql_tree = tile_gen_sql[0].sql
    sql_template = sql_tree.sql(pretty=True)
    sql_template = sql_template.replace("FBT_START_DATE", "'2022-04-18'")
    sql_template = sql_template.replace("FBT_END_DATE", "'2022-04-19'")
    print()
    print(sql_template)
