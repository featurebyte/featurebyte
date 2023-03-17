"""
Tests for Join SQLNode
"""
import textwrap

import pytest
from pydantic import ValidationError

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.generic import JoinNode
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter


@pytest.fixture(name="item_data_join_event_data_filtered_node")
def item_data_join_event_data_filtered_node_fixture(global_graph, item_data_join_event_data_node):
    """
    Apply filtering on a join node
    """
    graph = global_graph
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["item_type"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[item_data_join_event_data_node],
    )
    condition = graph.add_operation(
        node_type=NodeType.EQ,
        node_params={"value": "sports"},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_a],
    )
    filtered_node = graph.add_operation(
        node_type=NodeType.FILTER,
        node_params={},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_data_join_event_data_node, condition],
    )
    return filtered_node


@pytest.fixture
def derived_expression_from_join_node(global_graph, item_data_join_event_data_node):
    """
    ExpressionNode derived from a join node
    """
    project_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["item_id"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(item_data_join_event_data_node.name)],
    )
    add_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node],
    )
    return add_node


def test_item_data_join_event_data_attributes(global_graph, item_data_join_event_data_node):
    """
    Test SQL generation for ItemTable joined with EventTable
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(item_data_join_event_data_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          L."order_method" AS "order_method",
          R."order_id" AS "order_id",
          R."item_id" AS "item_id",
          R."item_name" AS "item_name",
          R."item_type" AS "item_type"
        FROM (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "order_id" AS "order_id",
            "order_method" AS "order_method"
          FROM "db"."public"."event_table"
        ) AS L
        INNER JOIN (
          SELECT
            "order_id" AS "order_id",
            "item_id" AS "item_id",
            "item_name" AS "item_name",
            "item_type" AS "item_type"
          FROM "db"."public"."item_table"
        ) AS R
          ON L."order_id" = R."order_id"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_item_data_join_event_data_attributes_with_filter(
    global_graph, item_data_join_event_data_filtered_node
):
    """
    Test SQL generation for ItemTable joined with EventTable with filter
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(item_data_join_event_data_filtered_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          L."order_method" AS "order_method",
          R."order_id" AS "order_id",
          R."item_id" AS "item_id",
          R."item_name" AS "item_name",
          R."item_type" AS "item_type"
        FROM (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "order_id" AS "order_id",
            "order_method" AS "order_method"
          FROM "db"."public"."event_table"
        ) AS L
        INNER JOIN (
          SELECT
            "order_id" AS "order_id",
            "item_id" AS "item_id",
            "item_name" AS "item_name",
            "item_type" AS "item_type"
          FROM "db"."public"."item_table"
        ) AS R
          ON L."order_id" = R."order_id"
        WHERE
          (
            R."item_type" = 'sports'
          )
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_item_data_join_event_data_attributes_on_demand_tile_gen(
    global_graph, item_data_joined_event_data_feature_node
):
    """
    Test on-demand tile SQL generation for ItemView
    """
    interpreter = GraphInterpreter(global_graph, SourceType.SNOWFLAKE)
    groupby_node_name = global_graph.get_input_node_names(item_data_joined_event_data_feature_node)[
        0
    ]
    groupby_node = global_graph.get_node_by_name(groupby_node_name)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(
            DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMPNTZ)) + tile_index * 3600
          ) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          "item_type",
          COUNT(*) AS value_count_d3aca2b274b8253dd5a2aa41d62f55757aa2f2ce
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
                L."order_method" AS "order_method",
                R."order_id" AS "order_id",
                R."item_id" AS "item_id",
                R."item_name" AS "item_name",
                R."item_type" AS "item_type"
              FROM (
                SELECT
                  "ts" AS "ts",
                  "cust_id" AS "cust_id",
                  "order_id" AS "order_id",
                  "order_method" AS "order_method"
                FROM "db"."public"."event_table"
              ) AS L
              INNER JOIN (
                SELECT
                  "order_id" AS "order_id",
                  "item_id" AS "item_id",
                  "item_name" AS "item_name",
                  "item_type" AS "item_type"
                FROM "db"."public"."item_table"
              ) AS R
                ON L."order_id" = R."order_id"
            ) AS R
              ON R."cust_id" = __FB_ENTITY_TABLE_NAME."cust_id"
              AND R."ts" >= __FB_START_DATE
              AND R."ts" < __FB_ENTITY_TABLE_NAME.__FB_ENTITY_TABLE_END_DATE
          )
        )
        GROUP BY
          tile_index,
          "cust_id",
          "item_type"
        """
    ).strip()
    assert tile_gen_sqls[0].sql == expected


def test_item_groupby_feature_joined_event_view(global_graph, order_size_feature_join_node):
    """
    Test SQL generation for non-time aware feature in ItemTable joined into EventView
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(order_size_feature_join_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          L."ts" AS "ts",
          L."cust_id" AS "cust_id",
          L."order_id" AS "order_id",
          L."order_method" AS "order_method",
          R."__FB_TEMP_FEATURE_NAME" AS "ord_size"
        FROM (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "order_id" AS "order_id",
            "order_method" AS "order_method"
          FROM "db"."public"."event_table"
        ) AS L
        LEFT JOIN (
          SELECT
            (
              "order_size" + 123
            ) AS "__FB_TEMP_FEATURE_NAME",
            "order_id"
          FROM (
            SELECT
              "order_id" AS "order_id",
              COUNT(*) AS "order_size"
            FROM (
              SELECT
                "order_id" AS "order_id",
                "item_id" AS "item_id",
                "item_name" AS "item_name",
                "item_type" AS "item_type"
              FROM "db"."public"."item_table"
            )
            GROUP BY
              "order_id"
          )
        ) AS R
          ON L."order_id" = R."order_id"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_derived_expression_from_join_node(global_graph, derived_expression_from_join_node):
    """
    Test derived expression from join node
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(derived_expression_from_join_node).sql_standalone
    expected = textwrap.dedent(
        """
        SELECT
          (
            R."item_id" + 123
          )
        FROM (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "order_id" AS "order_id",
            "order_method" AS "order_method"
          FROM "db"."public"."event_table"
        ) AS L
        INNER JOIN (
          SELECT
            "order_id" AS "order_id",
            "item_id" AS "item_id",
            "item_name" AS "item_name",
            "item_type" AS "item_type"
          FROM "db"."public"."item_table"
        ) AS R
          ON L."order_id" = R."order_id"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_double_aggregation(global_graph, order_size_agg_by_cust_id_graph):
    """
    Test aggregating a non-time aware feature derived from ItemTable
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.BUILD_TILE, source_type=SourceType.SNOWFLAKE
    )
    _, node = order_size_agg_by_cust_id_graph
    sql_tree = sql_graph.build(node).sql
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("ord_size") AS sum_value_avg_5c419f6baffa4a4a158e33527e5c5a288dc03d42,
          COUNT("ord_size") AS count_value_avg_5c419f6baffa4a4a158e33527e5c5a288dc03d42
        FROM (
          SELECT
            *,
            FLOOR(
              (
                DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))
              ) / 3600
            ) AS tile_index
          FROM (
            SELECT
              *
            FROM (
              SELECT
                L."ts" AS "ts",
                L."cust_id" AS "cust_id",
                L."order_id" AS "order_id",
                L."order_method" AS "order_method",
                R."__FB_TEMP_FEATURE_NAME" AS "ord_size"
              FROM (
                SELECT
                  "ts" AS "ts",
                  "cust_id" AS "cust_id",
                  "order_id" AS "order_id",
                  "order_method" AS "order_method"
                FROM "db"."public"."event_table"
              ) AS L
              LEFT JOIN (
                SELECT
                  (
                    "order_size" + 123
                  ) AS "__FB_TEMP_FEATURE_NAME",
                  "order_id"
                FROM (
                  SELECT
                    "order_id" AS "order_id",
                    COUNT(*) AS "order_size"
                  FROM (
                    SELECT
                      "order_id" AS "order_id",
                      "item_id" AS "item_id",
                      "item_name" AS "item_name",
                      "item_type" AS "item_type"
                    FROM "db"."public"."item_table"
                  )
                  GROUP BY
                    "order_id"
                )
              ) AS R
                ON L."order_id" = R."order_id"
            )
            WHERE
              "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
              AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
          )
        )
        GROUP BY
          tile_index,
          "cust_id"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_scd_join(global_graph, scd_join_node):
    """
    Test SQL generation for SCD join
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(scd_join_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          L."event_timestamp" AS "event_timestamp",
          L."cust_id" AS "cust_id",
          L."event_column_1_out" AS "event_column_1_out",
          L."event_column_2_out" AS "event_column_2_out",
          R."membership_status" AS "latest_membership_status"
        FROM (
          SELECT
            "__FB_KEY_COL_0",
            "__FB_LAST_TS",
            "event_timestamp",
            "cust_id",
            "event_column_1_out",
            "event_column_2_out"
          FROM (
            SELECT
              "__FB_KEY_COL_0",
              LAG("__FB_EFFECTIVE_TS_COL") IGNORE NULLS OVER (PARTITION BY "__FB_KEY_COL_0" ORDER BY "__FB_TS_COL" NULLS LAST, "__FB_TS_TIE_BREAKER_COL" NULLS LAST) AS "__FB_LAST_TS",
              "event_timestamp",
              "cust_id",
              "event_column_1_out",
              "event_column_2_out",
              "__FB_EFFECTIVE_TS_COL"
            FROM (
              SELECT
                CAST(CONVERT_TIMEZONE('UTC', "event_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
                "cust_id" AS "__FB_KEY_COL_0",
                NULL AS "__FB_EFFECTIVE_TS_COL",
                2 AS "__FB_TS_TIE_BREAKER_COL",
                "event_timestamp" AS "event_timestamp",
                "cust_id" AS "cust_id",
                "event_column_1" AS "event_column_1_out",
                "event_column_2" AS "event_column_2_out"
              FROM (
                SELECT
                  "ts" AS "ts",
                  "cust_id" AS "cust_id",
                  "order_id" AS "order_id",
                  "order_method" AS "order_method"
                FROM "db"."public"."event_table"
              )
              UNION ALL
              SELECT
                CAST(CONVERT_TIMEZONE('UTC', "effective_timestamp") AS TIMESTAMP) AS "__FB_TS_COL",
                "cust_id" AS "__FB_KEY_COL_0",
                "effective_timestamp" AS "__FB_EFFECTIVE_TS_COL",
                1 AS "__FB_TS_TIE_BREAKER_COL",
                NULL AS "event_timestamp",
                NULL AS "cust_id",
                NULL AS "event_column_1_out",
                NULL AS "event_column_2_out"
              FROM (
                SELECT
                  "effective_ts" AS "effective_ts",
                  "cust_id" AS "cust_id",
                  "membership_status" AS "membership_status"
                FROM "db"."public"."customer_profile_table"
              )
            )
          )
          WHERE
            "__FB_EFFECTIVE_TS_COL" IS NULL
        ) AS L
        LEFT JOIN (
          SELECT
            "effective_ts" AS "effective_ts",
            "cust_id" AS "cust_id",
            "membership_status" AS "membership_status"
          FROM "db"."public"."customer_profile_table"
        ) AS R
          ON L."__FB_LAST_TS" = R."effective_timestamp" AND L."__FB_KEY_COL_0" = R."cust_id"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


@pytest.mark.parametrize(
    "param_name",
    ["left_input_columns", "left_output_columns", "right_input_columns", "right_output_columns"],
)
def test_join_node_parameters_validation__duplicated_columns(param_name, join_node_params):
    """Test join node parameter validation logic"""
    with pytest.raises(ValidationError) as exc:
        JoinNode(parameters={**join_node_params, param_name: ["a", "a"]})
    expected_msg = "Column names (values: ['a', 'a']) must be unique! (type=value_error)"
    assert expected_msg in str(exc.value)


def test_join_node_parameters_validation__overlapped_columns(join_node_params):
    """Test join node parameter validation logic"""
    with pytest.raises(ValidationError) as exc:
        JoinNode(
            parameters={
                **join_node_params,
                "right_output_columns": join_node_params["left_output_columns"],
            }
        )
    expected_msg = (
        "Left and right output columns should not have common item(s). (type=value_error)"
    )
    assert expected_msg in str(exc.value)
