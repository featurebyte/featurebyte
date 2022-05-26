"""
This module contains the tests for the Query Graph Interpreter
"""
import textwrap
from dataclasses import asdict

import pytest

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.interpreter import GraphInterpreter, SQLOperationGraph


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """
    Empty query graph fixture
    """
    QueryGraph.clear()
    yield QueryGraph()


def test_graph_interpreter_super_simple(graph):
    """Test using a simple query graph"""
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    assign = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "a_copy"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, proj_a],
    )
    sql_graph = SQLOperationGraph(graph)
    sql_graph.build(graph.nodes[assign.name])
    sql_tree = sql_graph.get_node(assign.name).sql
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
    """Test using a slightly more complex graph (multiple assigns)"""
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    sum_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, sum_node],
    )
    proj_c = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["c"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[assign_node],
    )
    assign_node_2 = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c2"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, proj_c],
    )
    name = assign_node_2.name
    sql_graph = SQLOperationGraph(graph)
    sql_graph.build(graph.nodes[name])
    sql_tree = sql_graph.get_node(name).sql
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
    """Test tile building SQL"""
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["ts", "cust_id", "a", "b"],
            "timestamp": "ts",
            "dbtable": "event_table",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    sum_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, sum_node],
    )
    _groupby_node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            "key": "cust_id",
            "parent": "a",
            "agg_func": "sum",
            "window_end": 5,
            "frequency": 30,
            "blind_spot": 1,
            "timestamp": "ts",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    interpreter = GraphInterpreter(graph)
    tile_gen_sqls = interpreter.construct_tile_gen_sql()
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("sql")
    assert info_dict == {
        "columns": ["tile_start_date", "cust_id", "value"],
        "window_end": 5,
        "frequency": 30,
        "blind_spot": 1,
    }


def test_graph_interpreter_snowflake(graph):
    """Test tile building SQL and generates a SQL runnable on Snowflake"""
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["SERVER_TIMESTAMP", "CUST_ID"],
            "timestamp": "SERVER_TIMESTAMP",
            "dbtable": '"FB_SIMULATE"."PUBLIC"."BROWSING_TS"',
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    _groupby_node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            "key": "CUST_ID",
            "parent": "*",
            "agg_func": "COUNT",
            "window_end": 600,
            "frequency": 3600,
            "blind_spot": 1,
            "timestamp": "SERVER_TIMESTAMP",
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    interpreter = GraphInterpreter(graph)
    tile_gen_sql = interpreter.construct_tile_gen_sql()
    assert len(tile_gen_sql) == 1
    sql_template = tile_gen_sql[0].sql
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(FBT_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS tile_start_date,
          CUST_ID,
          COUNT(*) AS value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, SERVER_TIMESTAMP) - DATE_PART(EPOCH_SECOND, CAST(FBT_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                SELECT
                  SERVER_TIMESTAMP,
                  CUST_ID
                FROM "FB_SIMULATE"."PUBLIC"."BROWSING_TS"
                WHERE
                  SERVER_TIMESTAMP >= CAST(FBT_START_DATE AS TIMESTAMP)
                  AND SERVER_TIMESTAMP < CAST(FBT_END_DATE AS TIMESTAMP)
            )
        )
        GROUP BY
          tile_index,
          CUST_ID
        ORDER BY
          tile_index
        """
    ).strip()
    assert sql_template == expected

    # runnable directly in snowflake for testing
    sql_template = sql_template.replace("FBT_START_DATE", "'2022-04-18 00:00:00'")
    sql_template = sql_template.replace("FBT_END_DATE", "'2022-04-19 00:00:00'")
    print()
    print(sql_template)
