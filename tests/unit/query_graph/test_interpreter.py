"""
This module contains the tests for the Query Graph Interpreter
"""
import textwrap
from dataclasses import asdict

import pandas as pd
import pytest

from featurebyte.enum import InternalName, SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import GlobalGraphState, GlobalQueryGraph
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier


@pytest.fixture(name="graph", scope="function")
def query_graph():
    """
    Empty query graph fixture
    """
    GlobalGraphState.reset()
    yield GlobalQueryGraph()


@pytest.fixture(name="node_input")
def node_input_fixture(graph):
    """Fixture for a generic input node"""
    node_params = {
        "type": "event_data",
        "columns": ["ts", "cust_id", "a", "b"],
        "timestamp_column": "ts",
        "table_details": {
            "database_name": "db",
            "schema_name": "public",
            "table_name": "event_table",
        },
        "feature_store_details": {
            "type": "snowflake",
            "details": {
                "database": "db",
                "sf_schema": "public",
                "account": "account",
                "warehouse": "warehouse",
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


@pytest.fixture(name="simple_graph", scope="function")
def simple_graph_fixture(graph, node_input):
    """Simple graph"""
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
    return graph, assign


def test_graph_interpreter_super_simple(simple_graph):
    """Test using a simple query graph"""
    graph, node = simple_graph
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(node).sql
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


def test_graph_interpreter_assign_scalar(graph, node_input):
    """Test using a simple query graph"""
    assign = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "x", "value": 123},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(assign).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          123 AS "x"
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
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.BUILD_TILE, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(assign_node_2).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          (
            "a" + "b"
          ) AS "c",
          (
            "a" + "b"
          ) AS "c2"
        FROM "db"."public"."event_table"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "node_type, expected_expr",
    [
        (NodeType.ADD, '"a" + 123'),
        (NodeType.SUB, '"a" - 123'),
        (NodeType.MUL, '"a" * 123'),
        (NodeType.DIV, '"a" / NULLIF(123, 0)'),
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
    sql_graph = SQLOperationGraph(graph, SQLType.BUILD_TILE, source_type=SourceType.SNOWFLAKE)
    sql_tree = sql_graph.build(assign_node).sql
    expected = textwrap.dedent(
        f"""
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          (
            {expected_expr}
          ) AS "a2"
        FROM "db"."public"."event_table"
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
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
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


def test_graph_interpreter_tile_gen(query_graph_with_groupby, groupby_node_aggregation_id):
    """Test tile building SQL"""
    interpreter = GraphInterpreter(query_graph_with_groupby, SourceType.SNOWFLAKE)
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("sql_template")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "aggregation_id": f"avg_{groupby_node_aggregation_id}",
        "columns": [
            InternalName.TILE_START_DATE.value,
            "cust_id",
            f"sum_value_avg_{groupby_node_aggregation_id}",
            f"count_value_avg_{groupby_node_aggregation_id}",
        ],
        "time_modulo_frequency": 1800,
        "entity_columns": ["cust_id"],
        "tile_value_columns": [
            f"sum_value_avg_{groupby_node_aggregation_id}",
            f"count_value_avg_{groupby_node_aggregation_id}",
        ],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": None,
    }


def test_graph_interpreter_on_demand_tile_gen(
    query_graph_with_groupby, groupby_node_aggregation_id
):
    """Test tile building SQL with on-demand tile generation

    Note that the input table query contains a inner-join with an entity table to filter only data
    belonging to specific entity IDs and date range
    """
    interpreter = GraphInterpreter(query_graph_with_groupby, SourceType.SNOWFLAKE)
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)

    sql = tile_gen_sqls[0].sql
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("a") AS sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489,
          COUNT("a") AS count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            WITH __FB_ENTITY_TABLE_NAME AS (
              __FB_ENTITY_TABLE_SQL_PLACEHOLDER
            )
            SELECT
              R.*
            FROM __FB_ENTITY_TABLE_NAME
            INNER JOIN (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "a" AS "a",
                "b" AS "b",
                (
                  "a" + "b"
                ) AS "c"
              FROM "db"."public"."event_table"
            ) AS R
              ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
              AND R."ts" >= __FB_START_DATE
              AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
          )
        )
        GROUP BY
          tile_index,
          "cust_id"
        """
    ).strip()
    assert sql == expected_sql
    info_dict.pop("sql_template")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "aggregation_id": f"avg_{groupby_node_aggregation_id}",
        "columns": [
            InternalName.TILE_START_DATE.value,
            "cust_id",
            f"sum_value_avg_{groupby_node_aggregation_id}",
            f"count_value_avg_{groupby_node_aggregation_id}",
        ],
        "time_modulo_frequency": 1800,
        "entity_columns": ["cust_id"],
        "tile_value_columns": [
            f"sum_value_avg_{groupby_node_aggregation_id}",
            f"count_value_avg_{groupby_node_aggregation_id}",
        ],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": None,
    }


def test_graph_interpreter_tile_gen_with_category(query_graph_with_category_groupby):
    """Test tile building SQL with aggregation per category"""
    interpreter = GraphInterpreter(query_graph_with_category_groupby, SourceType.SNOWFLAKE)
    groupby_node = query_graph_with_category_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)

    aggregation_id = "658906478ded8ef60deb5e8aca90f8ef7d06c2c6"
    expected_sql = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "product_type",
          SUM("a") AS sum_value_avg_{aggregation_id},
          COUNT("a") AS count_value_avg_{aggregation_id}
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "a" AS "a",
                "b" AS "b",
                (
                  "a" + "b"
                ) AS "c"
              FROM "db"."public"."event_table"
            )
            WHERE
              "ts" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "ts" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          tile_index,
          "cust_id",
          "product_type"
        """
    ).strip()
    assert info.sql == expected_sql
    info_dict.pop("sql_template")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226",
        "aggregation_id": f"avg_{aggregation_id}",
        "columns": [
            InternalName.TILE_START_DATE.value,
            "cust_id",
            f"sum_value_avg_{aggregation_id}",
            f"count_value_avg_{aggregation_id}",
        ],
        "time_modulo_frequency": 1800,
        "entity_columns": ["cust_id"],
        "tile_value_columns": [
            f"sum_value_avg_{aggregation_id}",
            f"count_value_avg_{aggregation_id}",
        ],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": "product_type",
    }


def test_graph_interpreter_on_demand_tile_gen_two_groupby(
    complex_feature_query_graph, groupby_node_aggregation_id
):
    """Test case for a complex feature that depends on two groupby nodes"""
    complex_feature_node, graph = complex_feature_query_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(complex_feature_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 2

    # Check required tile 1 (groupby keys: cust_id)
    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    sql = info.sql
    info_dict.pop("sql_template")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "aggregation_id": f"avg_{groupby_node_aggregation_id}",
        "columns": [
            "__FB_TILE_START_DATE_COLUMN",
            "cust_id",
            f"sum_value_avg_{groupby_node_aggregation_id}",
            f"count_value_avg_{groupby_node_aggregation_id}",
        ],
        "entity_columns": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": None,
        "tile_value_columns": [
            f"sum_value_avg_{groupby_node_aggregation_id}",
            f"count_value_avg_{groupby_node_aggregation_id}",
        ],
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
    }
    expected = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("a") AS sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489,
          COUNT("a") AS count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            WITH __FB_ENTITY_TABLE_NAME AS (
              __FB_ENTITY_TABLE_SQL_PLACEHOLDER
            )
            SELECT
              R.*
            FROM __FB_ENTITY_TABLE_NAME
            INNER JOIN (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "a" AS "a",
                "b" AS "b",
                (
                  "a" + "b"
                ) AS "c"
              FROM "db"."public"."event_table"
            ) AS R
              ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
              AND R."ts" >= __FB_START_DATE
              AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
          )
        )
        GROUP BY
          tile_index,
          "cust_id"
        """
    ).strip()
    assert sql == expected

    # Check required tile 2 (groupby keys: biz_id)
    aggregation_id = "875069c3061f4fbb8c0e49a0a927676315f07a46"
    info = tile_gen_sqls[1]
    info_dict = asdict(info)
    sql = info.sql
    info_dict.pop("sql_template")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624",
        "aggregation_id": f"sum_{aggregation_id}",
        "columns": [
            "__FB_TILE_START_DATE_COLUMN",
            "biz_id",
            f"value_sum_{aggregation_id}",
        ],
        "entity_columns": ["biz_id"],
        "serving_names": ["BUSINESS_ID"],
        "value_by_column": None,
        "tile_value_columns": [f"value_sum_{aggregation_id}"],
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["7d"],
    }
    expected = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "biz_id",
          SUM("a") AS value_sum_875069c3061f4fbb8c0e49a0a927676315f07a46
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            WITH __FB_ENTITY_TABLE_NAME AS (
              __FB_ENTITY_TABLE_SQL_PLACEHOLDER
            )
            SELECT
              R.*
            FROM __FB_ENTITY_TABLE_NAME
            INNER JOIN (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "a" AS "a",
                "b" AS "b",
                (
                  "a" + "b"
                ) AS "c"
              FROM "db"."public"."event_table"
            ) AS R
              ON R."biz_id" = __FB_ENTITY_TABLE_NAME."biz_id"
              AND R."ts" >= __FB_START_DATE
              AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
          )
        )
        GROUP BY
          tile_index,
          "biz_id"
        """
    ).strip()
    assert sql == expected


def test_graph_interpreter_snowflake(graph):
    """Test tile building SQL and generates a SQL runnable on Snowflake"""
    node_input = graph.add_operation(
        node_type=NodeType.INPUT,
        node_params={
            "type": "event_data",
            "columns": ["SERVER_TIMESTAMP", "CUST_ID"],
            "timestamp_column": "SERVER_TIMESTAMP",
            "table_details": {
                "database_name": "FB_SIMULATE",
                "schema_name": "PUBLIC",
                "table_name": "BROWSING_TS",
            },
            "feature_store_details": {
                "type": "snowflake",
                "details": {
                    "database": "FB_SIMULATE",
                    "sf_schema": "PUBLIC",
                    "account": "account",
                    "warehouse": "warehouse",
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
        "names": ["feature_name"],
    }
    _groupby_node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **node_params,
            "tile_id": get_tile_table_identifier(
                row_index_lineage_hash="deadbeef1234", parameters=node_params
            ),
            "aggregation_id": get_aggregation_identifier(
                transformations_hash=graph.node_name_to_ref[node_input.name], parameters=node_params
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    tile_gen_sql = interpreter.construct_tile_gen_sql(_groupby_node, is_on_demand=False)
    assert len(tile_gen_sql) == 1
    sql_template = tile_gen_sql[0].sql
    expected = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "CUST_ID",
          COUNT(*) AS value_count_fc1ac0d8922166006b45c682224593ed1d15c3c6
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "SERVER_TIMESTAMP") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "SERVER_TIMESTAMP" AS "SERVER_TIMESTAMP",
                "CUST_ID" AS "CUST_ID"
              FROM "FB_SIMULATE"."PUBLIC"."BROWSING_TS"
            )
            WHERE
              "SERVER_TIMESTAMP" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "SERVER_TIMESTAMP" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          tile_index,
          "CUST_ID"
        """
    ).strip()
    assert sql_template == expected

    rendered_template = tile_gen_sql[0].sql_template.render(
        {
            InternalName.TILE_START_DATE_SQL_PLACEHOLDER: "2022-04-18 00:00:00",
            InternalName.TILE_END_DATE_SQL_PLACEHOLDER: "2022-04-19 00:00:00",
        },
    )
    expected = textwrap.dedent(
        f"""
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST('2022-04-18 00:00:00' AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "CUST_ID",
          COUNT(*) AS value_count_fc1ac0d8922166006b45c682224593ed1d15c3c6
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "SERVER_TIMESTAMP") - DATE_PART(EPOCH_SECOND, CAST('2022-04-18 00:00:00' AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "SERVER_TIMESTAMP" AS "SERVER_TIMESTAMP",
                "CUST_ID" AS "CUST_ID"
              FROM "FB_SIMULATE"."PUBLIC"."BROWSING_TS"
            )
            WHERE
              "SERVER_TIMESTAMP" >= CAST('2022-04-18 00:00:00' AS TIMESTAMPNTZ)
              AND "SERVER_TIMESTAMP" < CAST('2022-04-19 00:00:00' AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          tile_index,
          "CUST_ID"
        """
    ).strip()
    assert rendered_template == expected


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
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    sql_code = interpreter.construct_preview_sql("assign_2")[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          (
            "a" + "b"
          ) AS "c",
          (
            "a" + "b"
          ) AS "c2"
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected

    sql_code = interpreter.construct_preview_sql("add_1", 5)[0]
    expected = textwrap.dedent(
        """
        SELECT
          (
            "a" + "b"
          )
        FROM "db"."public"."event_table"
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
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(filter_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b"
        FROM "db"."public"."event_table"
        WHERE
          (
            "b" = 123
          )
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected

    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(filter_series_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "a"
        FROM "db"."public"."event_table"
        WHERE
          (
            "b" = 123
          )
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_multiple_filters(graph, node_input):
    """Test graph with filter operation"""
    proj_b = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["b"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    binary_node_1 = graph.add_operation(
        node_type=NodeType.GE,
        node_params={"value": 1000},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_b],
    )
    binary_node_2 = graph.add_operation(
        node_type=NodeType.LE,
        node_params={"value": 5000},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_b],
    )
    filter_node_1 = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, binary_node_1],
    )
    filter_node_2 = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[filter_node_1, binary_node_2],
    )
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(filter_node_2.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b"
        FROM "db"."public"."event_table"
        WHERE
          (
            "b" >= 1000
          ) AND (
            "b" <= 5000
          )
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
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(project_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "b" AS "b",
          (
            "b" = 123
          ) AS "new_col"
        FROM "db"."public"."event_table"
        WHERE
          (
            "b" = 123
          )
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
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(assign_node.name)[0]
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


def test_conditional_assign__project_named(graph, node_input):
    """Test graph with conditional assign operation"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    mask_node = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": -999},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    conditional_node = graph.add_operation(
        node_type=NodeType.CONDITIONAL,
        node_params={"value": 0},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a, mask_node],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "a"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, conditional_node],
    )
    projected_conditional = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[assign_node],
    )

    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(projected_conditional.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          CASE WHEN (
            "a" = -999
          ) THEN 0 ELSE "a" END AS "a"
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected

    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(assign_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          CASE WHEN (
            "a" = -999
          ) THEN 0 ELSE "a" END AS "a",
          "b" AS "b"
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_isnull(graph, node_input):
    """Test graph with isnull operation"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    mask_node = graph.add_operation(
        node_type=NodeType.IS_NULL,
        node_params={},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_preview_sql(mask_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "a" IS NULL
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_window_function(graph, node_input):
    """Test tile sql when window function is involved

    Note that the tile start and end date filters are applied on a nested subquery containing the
    window expression, not on the same select statement. This is so that the data required by
    the window expression is not filtered prematurely.
    """
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    binary_node = graph.add_operation(
        node_type=NodeType.GT,
        node_params={"value": 1000},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    filtered_input_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, binary_node],
    )
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[filtered_input_node],
    )
    lagged_a = graph.add_operation(
        node_type=NodeType.LAG,
        node_params={"timestamp_column": "ts", "entity_columns": ["cust_id"], "offset": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "prev_a"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[filtered_input_node, lagged_a],
    )
    node_params = {
        "keys": ["cust_id"],
        "value_by": None,
        "parent": "prev_a",
        "agg_func": "count",
        "time_modulo_frequency": 600,
        "frequency": 3600,
        "blind_spot": 1,
        "timestamp": "ts",
        "windows": ["1d"],
        "serving_names": ["cust_id"],
        "names": ["feature_name"],
    }
    groupby_node = graph.add_operation(
        node_type=NodeType.GROUPBY,
        node_params={
            **node_params,
            "tile_id": get_tile_table_identifier(
                row_index_lineage_hash="deadbeef1234", parameters=node_params
            ),
            "aggregation_id": get_aggregation_identifier(
                transformations_hash=graph.node_name_to_ref[assign_node.name],
                parameters=node_params,
            ),
        },
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node],
    )
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.BUILD_TILE, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(assign_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) AS "prev_a"
        FROM "db"."public"."event_table"
        WHERE
          (
            "a" > 1000
          )
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected

    # check generated sql for building tiles
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    tile_gen_sql = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          COUNT(*) AS value_count_7857b056f366bf32a56563964923dbf1f4c4f931
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                "ts" AS "ts",
                "cust_id" AS "cust_id",
                "a" AS "a",
                "b" AS "b",
                LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts") AS "prev_a"
              FROM "db"."public"."event_table"
              WHERE
                (
                  "a" > 1000
                )
            )
            WHERE
              "ts" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
              AND "ts" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
          )
        )
        GROUP BY
          tile_index,
          "cust_id"
        """
    ).strip()
    assert tile_gen_sql[0].sql == expected


def test_window_function__as_filter(graph, node_input):
    """Test when condition derived from window function is used as filter"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    lagged_a = graph.add_operation(
        node_type=NodeType.LAG,
        node_params={"timestamp_column": "ts", "entity_columns": ["cust_id"], "offset": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "prev_a"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, lagged_a],
    )
    proj_lagged_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["prev_a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[assign_node],
    )
    binary_node = graph.add_operation(
        node_type=NodeType.GT,
        node_params={"value": 0},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_lagged_a],
    )
    filtered_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, binary_node],
    )
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(filtered_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) AS "prev_a"
        FROM "db"."public"."event_table"
        QUALIFY
          (
            LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
          )
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_window_function__multiple_filters(graph, node_input):
    """Test when condition derived from window function is used as filter"""
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node_input],
    )
    non_window_based_condition = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    filtered_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input, non_window_based_condition],
    )
    lagged_a = graph.add_operation(
        node_type=NodeType.LAG,
        node_params={"timestamp_column": "ts", "entity_columns": ["cust_id"], "offset": 1},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    window_based_condition = graph.add_operation(
        node_type=NodeType.GT,
        node_params={"value": 0},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[lagged_a],
    )
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "prev_a"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[filtered_node, lagged_a],
    )
    filtered_node_2 = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[assign_node, window_based_condition],
    )
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(filtered_node_2).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) AS "prev_a"
        FROM "db"."public"."event_table"
        WHERE
          (
            "a" = 123
          )
        QUALIFY
          (
            LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
          )
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_databricks_source(query_graph_with_groupby):
    """Test SQL generation for databricks source"""
    graph = query_graph_with_groupby
    input_node = graph.get_node_by_name("input_1")
    groupby_node = graph.get_node_by_name("groupby_1")
    interpreter = GraphInterpreter(graph, source_type=SourceType.DATABRICKS)

    # Check preview SQL
    preview_sql = interpreter.construct_preview_sql(input_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          `ts` AS `ts`,
          `cust_id` AS `cust_id`,
          `a` AS `a`,
          `b` AS `b`
        FROM `db`.`public`.`event_table`
        LIMIT 10
        """
    ).strip()
    assert preview_sql == expected

    # Check tile SQL
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1
    tile_sql = tile_gen_sqls[0].sql
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(UNIX_TIMESTAMP(CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          `cust_id`,
          SUM(`a`) AS sum_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489,
          COUNT(`a`) AS count_value_avg_833762b783166cd0980c65b9e3f3c7c6b9dcd489
        FROM (
          SELECT
            *,
            FLOOR(
              (
                UNIX_TIMESTAMP(`ts`) - UNIX_TIMESTAMP(CAST(__FB_START_DATE AS TIMESTAMP))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                `ts` AS `ts`,
                `cust_id` AS `cust_id`,
                `a` AS `a`,
                `b` AS `b`,
                (
                  `a` + `b`
                ) AS `c`
              FROM `db`.`public`.`event_table`
            )
            WHERE
              `ts` >= CAST(__FB_START_DATE AS TIMESTAMP)
              AND `ts` < CAST(__FB_END_DATE AS TIMESTAMP)
          )
        )
        GROUP BY
          tile_index,
          `cust_id`
        """
    ).strip()
    assert tile_sql == expected


def test_tile_sql_order_dependent_aggregation(global_graph, latest_value_aggregation_feature_node):
    """
    Test generating tile sql for an order dependent aggregation
    """
    interpreter = GraphInterpreter(global_graph, source_type=SourceType.SNOWFLAKE)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(
        latest_value_aggregation_feature_node, is_on_demand=False
    )
    assert len(tile_gen_sqls) == 1
    tile_sql = tile_gen_sqls[0].sql
    expected = textwrap.dedent(
        """
        SELECT
          __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          value_latest_088635a8a233d93984ceb9acdaa23eaa1460f338
        FROM (
          SELECT
            TO_TIMESTAMP(
              DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
            ) AS __FB_TILE_START_DATE_COLUMN,
            "cust_id",
            ROW_NUMBER() OVER (PARTITION BY tile_index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS "__FB_ROW_NUMBER",
            FIRST_VALUE("a") OVER (PARTITION BY tile_index, "cust_id" ORDER BY "ts" DESC NULLS LAST) AS value_latest_088635a8a233d93984ceb9acdaa23eaa1460f338
          FROM (
            SELECT
              *,
              FLOOR(
                (
                  DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ))
                ) / 3600
              ) AS tile_index
            FROM (
              SELECT
                *
              FROM (
                SELECT
                  "ts" AS "ts",
                  "cust_id" AS "cust_id",
                  "a" AS "a",
                  "b" AS "b"
                FROM "db"."public"."event_table"
              )
              WHERE
                "ts" >= CAST(__FB_START_DATE AS TIMESTAMPNTZ)
                AND "ts" < CAST(__FB_END_DATE AS TIMESTAMPNTZ)
            )
          )
        )
        WHERE
          "__FB_ROW_NUMBER" = 1
        """
    ).strip()
    assert tile_sql == expected


def test_graph_interpreter_sample(simple_graph):
    """Test graph sample"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    sql_code = interpreter.construct_sample_sql(node.name, num_rows=10, seed=1234)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          "a" AS "a_copy"
        FROM "db"."public"."event_table"
        ORDER BY
          RANDOM(1234)
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_graph_interpreter_sample_date_range(simple_graph):
    """Test graph sample with date range"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    sql_code = interpreter.construct_sample_sql(
        node.name,
        num_rows=10,
        seed=10,
        timestamp_column="ts",
        from_timestamp=pd.to_datetime("2020-01-01"),
        to_timestamp=pd.to_datetime("2020-01-03"),
    )[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          "a" AS "a_copy"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= CAST('2020-01-01T00:00:00' AS TIMESTAMPNTZ)
          AND "ts" < CAST('2020-01-03T00:00:00' AS TIMESTAMPNTZ)
        ORDER BY
          RANDOM(10)
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_graph_interpreter_sample_date_range_no_timestamp_column(simple_graph):
    """Test graph sample with date range"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    sql_code = interpreter.construct_sample_sql(
        node.name,
        num_rows=30,
        seed=20,
        timestamp_column=None,
        from_timestamp=pd.to_datetime("2020-01-01"),
        to_timestamp=pd.to_datetime("2020-01-03"),
    )[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          "cust_id" AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          "a" AS "a_copy"
        FROM "db"."public"."event_table"
        ORDER BY
          RANDOM(20)
        LIMIT 30
        """
    ).strip()
    assert sql_code == expected
