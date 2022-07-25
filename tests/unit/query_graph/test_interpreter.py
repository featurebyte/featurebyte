"""
This module contains the tests for the Query Graph Interpreter
"""
import textwrap
from dataclasses import asdict

import pytest

from featurebyte.enum import InternalName
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalQueryGraph, GlobalQueryGraphState
from featurebyte.query_graph.interpreter import GraphInterpreter, SQLOperationGraph, SQLType
from featurebyte.query_graph.util import get_tile_table_identifier


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """
    Empty query graph fixture
    """
    GlobalQueryGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="node_input")
def node_input_fixture(graph):
    """Fixture for a generic input node"""
    node_params = {
        "columns": ["ts", "cust_id", "a", "b"],
        "timestamp": "ts",
        "dbtable": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "event_table",
        },
        "database_source": {
            "type": "snowflake",
            "details": {
                "database": "db",
                "sf_schema": "public",
            },
        },
    }
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


def test_graph_interpreter_super_simple(graph, node_input):
    """Test using a simple query graph"""
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
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.EVENT_VIEW_PREVIEW)
    sql_tree = sql_graph.build(assign).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          "a" AS "a_copy"
        FROM "db"."public"."event_table"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_graph_interpreter_multi_assign(graph, node_input):
    """Test using a slightly more complex graph (multiple assigns)"""
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
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.BUILD_TILE)
    sql_tree = sql_graph.build(assign_node_2).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          ("a" + "b") AS "c",
          ("a" + "b") AS "c2"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
          AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "node_type, expected_expr",
    [
        (NodeType.ADD, '"a" + 123'),
        (NodeType.SUB, '"a" - 123'),
        (NodeType.MUL, '"a" * 123'),
        (NodeType.DIV, '"a" / 123'),
        (NodeType.EQ, '"a" = 123'),
        (NodeType.NE, '"a" <> 123'),
        (NodeType.LT, '"a" < 123'),
        (NodeType.LE, '"a" <= 123'),
        (NodeType.GT, '"a" > 123'),
        (NodeType.GE, '"a" >= 123'),
        (NodeType.AND, '"a" AND 123'),
        (NodeType.OR, '"a" OR 123'),
    ],
)
def test_graph_interpreter_binary_operations(graph, node_input, node_type, expected_expr):
    """Test graph with binary operation nodes"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    binary_node = graph.add_operation(
        node_type=node_type,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "a2"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, binary_node],
    )
    sql_graph = SQLOperationGraph(graph, SQLType.BUILD_TILE)
    sql_tree = sql_graph.build(assign_node).sql
    expected = textwrap.dedent(
        f"""
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          ({expected_expr}) AS "a2"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
          AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_graph_interpreter_project_multiple_columns(graph, node_input):
    """Test using a simple query graph"""
    proj = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a", "b"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.EVENT_VIEW_PREVIEW)
    sql_tree = sql_graph.build(proj).sql
    expected = textwrap.dedent(
        """
        SELECT
          "a" AS "a",
          "b" AS "b"
        FROM "db"."public"."event_table"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_graph_interpreter_tile_gen(query_graph_with_groupby):
    """Test tile building SQL"""
    interpreter = GraphInterpreter(query_graph_with_groupby)
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("sql")
    assert info_dict == {
        "tile_table_id": "avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89",
        "columns": [InternalName.TILE_START_DATE.value, "cust_id", "sum_value", "count_value"],
        "time_modulo_frequency": 1800,
        "entity_columns": ["cust_id"],
        "tile_value_columns": ["sum_value", "count_value"],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
    }


def test_graph_interpreter_on_demand_tile_gen(query_graph_with_groupby):
    """Test tile building SQL with on-demand tile generation

    Note that the input table query contains a left-join with an entity table to filter only data
    belonging to specific entity IDs and date range
    """
    interpreter = GraphInterpreter(query_graph_with_groupby)
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)

    sql = info_dict.pop("sql")
    expected_sql = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_ENTITY_TABLE_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("a") AS sum_value,
          COUNT(*) AS count_value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_ENTITY_TABLE_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                WITH __FB_ENTITY_TABLE_NAME AS (
                    __FB_ENTITY_TABLE_SQL_PLACEHOLDER
                )
                SELECT
                  R.*,
                  __FB_ENTITY_TABLE_START_DATE
                FROM __FB_ENTITY_TABLE_NAME
                LEFT JOIN (
                    SELECT
                      "ts" AS "ts",
                      "cust_id" AS "cust_id",
                      "a" AS "a",
                      "b" AS "b",
                      ("a" + "b") AS "c"
                    FROM "db"."public"."event_table"
                ) AS R
                  ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
                  AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
                  AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
            )
        )
        GROUP BY
          tile_index,
          "cust_id",
          __FB_ENTITY_TABLE_START_DATE
        ORDER BY
          tile_index
        """
    ).strip()
    assert sql == expected_sql
    assert info_dict == {
        "tile_table_id": "avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89",
        "columns": [InternalName.TILE_START_DATE.value, "cust_id", "sum_value", "count_value"],
        "time_modulo_frequency": 1800,
        "entity_columns": ["cust_id"],
        "tile_value_columns": ["sum_value", "count_value"],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
    }


def test_graph_interpreter_tile_gen_with_category(query_graph_with_category_groupby):
    """Test tile building SQL with aggregation per category"""
    interpreter = GraphInterpreter(query_graph_with_category_groupby)
    groupby_node = query_graph_with_category_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)

    sql = info_dict.pop("sql")
    expected_sql = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "product_type",
          SUM("a") AS sum_value,
          COUNT(*) AS count_value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                SELECT
                  "ts" AS "ts",
                  "cust_id" AS "cust_id",
                  "a" AS "a",
                  "b" AS "b",
                  ("a" + "b") AS "c"
                FROM "db"."public"."event_table"
                WHERE
                  "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
                  AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
            )
        )
        GROUP BY
          tile_index,
          "cust_id",
          "product_type"
        ORDER BY
          tile_index
        """
    ).strip()
    assert sql == expected_sql
    assert info_dict == {
        "tile_table_id": "avg_f3600_m1800_b900_8c9dd5af3427568b4cddd3244d1461b16011b34d",
        "columns": [InternalName.TILE_START_DATE.value, "cust_id", "sum_value", "count_value"],
        "time_modulo_frequency": 1800,
        "entity_columns": ["cust_id"],
        "tile_value_columns": ["sum_value", "count_value"],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
    }


def test_graph_interpreter_on_demand_tile_gen_two_groupby(complex_feature_query_graph):
    """Test case for a complex feature that depends on two groupby nodes"""
    complex_feature_node, graph = complex_feature_query_graph
    interpreter = GraphInterpreter(graph)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(complex_feature_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 2

    # Check required tile 1 (groupby keys: cust_id)
    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    sql = info_dict.pop("sql")
    assert info_dict == {
        "tile_table_id": "avg_f3600_m1800_b900_588d3ccc5cb315d92899138db4670ae954d01b89",
        "columns": ["__FB_TILE_START_DATE_COLUMN", "cust_id", "sum_value", "count_value"],
        "entity_columns": ["cust_id"],
        "tile_value_columns": ["sum_value", "count_value"],
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
    }
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_ENTITY_TABLE_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("a") AS sum_value,
          COUNT(*) AS count_value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_ENTITY_TABLE_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                WITH __FB_ENTITY_TABLE_NAME AS (
                    __FB_ENTITY_TABLE_SQL_PLACEHOLDER
                )
                SELECT
                  R.*,
                  __FB_ENTITY_TABLE_START_DATE
                FROM __FB_ENTITY_TABLE_NAME
                LEFT JOIN (
                    SELECT
                      "ts" AS "ts",
                      "cust_id" AS "cust_id",
                      "a" AS "a",
                      "b" AS "b",
                      ("a" + "b") AS "c"
                    FROM "db"."public"."event_table"
                ) AS R
                  ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
                  AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
                  AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
            )
        )
        GROUP BY
          tile_index,
          "cust_id",
          __FB_ENTITY_TABLE_START_DATE
        ORDER BY
          tile_index
        """
    ).strip()
    assert sql == expected

    # Check required tile 2 (groupby keys: biz_id)
    info = tile_gen_sqls[1]
    info_dict = asdict(info)
    sql = info_dict.pop("sql")
    assert info_dict == {
        "tile_table_id": "sum_f3600_m1800_b900_650c6aa57569a2e01436fbf89014df6d17a21be7",
        "columns": ["__FB_TILE_START_DATE_COLUMN", "biz_id", "value"],
        "entity_columns": ["biz_id"],
        "tile_value_columns": ["value"],
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["a_7d_sum_by_business"],
        "serving_names": ["BUSINESS_ID"],
    }
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_ENTITY_TABLE_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "biz_id",
          SUM("a") AS value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_ENTITY_TABLE_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                WITH __FB_ENTITY_TABLE_NAME AS (
                    __FB_ENTITY_TABLE_SQL_PLACEHOLDER
                )
                SELECT
                  R.*,
                  __FB_ENTITY_TABLE_START_DATE
                FROM __FB_ENTITY_TABLE_NAME
                LEFT JOIN (
                    SELECT
                      "ts" AS "ts",
                      "cust_id" AS "cust_id",
                      "a" AS "a",
                      "b" AS "b",
                      ("a" + "b") AS "c"
                    FROM "db"."public"."event_table"
                ) AS R
                  ON R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
                  AND R."ts" >= __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_START_DATE
                  AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
            )
        )
        GROUP BY
          tile_index,
          "biz_id",
          __FB_ENTITY_TABLE_START_DATE
        ORDER BY
          tile_index
        """
    ).strip()
    assert sql == expected


def test_graph_interpreter_snowflake(graph):
    """Test tile building SQL and generates a SQL runnable on Snowflake"""
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "columns": ["SERVER_TIMESTAMP", "CUST_ID"],
            "timestamp": "SERVER_TIMESTAMP",
            "dbtable": {
                "database_name": "FB_SIMULATE",
                "schema_name": "PUBLIC",
                "table_name": "BROWSING_TS",
            },
            "database_source": {
                "type": "snowflake",
                "details": {
                    "database": "FB_SIMULATE",
                    "sf_schema": "PUBLIC",
                },
            },
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    node_params = {
        "keys": ["CUST_ID"],
        "value_by": None,
        "parent": "*",
        "agg_func": "count",
        "time_modulo_frequency": 600,
        "frequency": 3600,
        "blind_spot": 1,
        "timestamp": "SERVER_TIMESTAMP",
        "windows": ["1d"],
        "serving_names": ["CUSTOMER_ID"],
    }
    _groupby_node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **node_params,
            "tile_id": get_tile_table_identifier(
                transformations_hash=graph.node_name_to_ref[node_input.name], parameters=node_params
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    interpreter = GraphInterpreter(graph)
    tile_gen_sql = interpreter.construct_tile_gen_sql(_groupby_node, is_on_demand=False)
    assert len(tile_gen_sql) == 1
    sql_template = tile_gen_sql[0].sql
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "CUST_ID",
          COUNT(*) AS value
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "SERVER_TIMESTAMP") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                SELECT
                  "SERVER_TIMESTAMP" AS "SERVER_TIMESTAMP",
                  "CUST_ID" AS "CUST_ID"
                FROM "FB_SIMULATE"."PUBLIC"."BROWSING_TS"
                WHERE
                  "SERVER_TIMESTAMP" >= CAST(__FB_START_DATE AS TIMESTAMP)
                  AND "SERVER_TIMESTAMP" < CAST(__FB_END_DATE AS TIMESTAMP)
            )
        )
        GROUP BY
          tile_index,
          "CUST_ID"
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


def test_graph_interpreter_preview(graph, node_input):
    """Test graph preview"""
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
    add_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, proj_b],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "c"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, add_node],
    )
    proj_c = graph.add_operation(  # project_3
        node_type=NodeType.PROJECT,
        node_params={"columns": ["c"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[assign_node],
    )
    _assign_node_2 = graph.add_operation(  # assign_2
        node_type=NodeType.ASSIGN,
        node_params={"name": "c2"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, proj_c],
    )
    interpreter = GraphInterpreter(graph)

    sql_code = interpreter.construct_preview_sql("assign_2")
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          ("a" + "b") AS "c",
          ("a" + "b") AS "c2"
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected

    sql_code = interpreter.construct_preview_sql("add_1", 5)
    expected = textwrap.dedent(
        """
        SELECT
          ("a" + "b")
        FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "a" AS "a",
              "b" AS "b"
            FROM "db"."public"."event_table"
        )
        LIMIT 5
        """
    ).strip()
    assert sql_code == expected


def test_filter_node(graph, node_input):
    """Test graph with filter operation"""
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
    binary_node = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_b],
    )
    filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, binary_node],
    )
    filter_series_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, binary_node],
    )
    interpreter = GraphInterpreter(graph)
    sql_code = interpreter.construct_preview_sql(filter_node.name)
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b"
        FROM "db"."public"."event_table"
        WHERE
          ("b" = 123)
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected

    interpreter = GraphInterpreter(graph)
    sql_code = interpreter.construct_preview_sql(filter_series_node.name)
    expected = textwrap.dedent(
        """
        SELECT
          "a"
        FROM (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "a" AS "a",
              "b" AS "b"
            FROM "db"."public"."event_table"
        )
        WHERE
          ("b" = 123)
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_filter_assign_project(graph, node_input):
    """Test graph with both filter, assign, and project operations"""
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    binary_node = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_b],
    )
    filter_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, binary_node],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "new_col"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[filter_node, binary_node],
    )
    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b", "new_col"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    interpreter = GraphInterpreter(graph)
    sql_code = interpreter.construct_preview_sql(project_node.name)
    expected = textwrap.dedent(
        """
        SELECT
          "b" AS "b",
          ("b" = 123) AS "new_col"
        FROM "db"."public"."event_table"
        WHERE
          ("b" = 123)
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_project_multi_then_assign(graph, node_input):
    """Test graph with both projection and assign operations"""
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["ts", "a"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "new_col"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[project_node, proj_b],
    )
    interpreter = GraphInterpreter(graph)
    sql_code = interpreter.construct_preview_sql(assign_node.name)
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "a" AS "a",
          "b" AS "new_col"
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected
