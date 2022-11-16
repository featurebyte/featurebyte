"""
Tests for Join SQLNode
"""
import textwrap

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType


def test_item_data_join_event_data_attributes(global_graph, item_data_join_event_data_node):
    """
    Test SQL generation for ItemData joined with EventData
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
        LEFT JOIN (
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


def test_item_groupby_feature_joined_event_view(global_graph, order_size_feature_join_node):
    """
    Test SQL generation for non-time aware feature in ItemData joined into EventView
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
          R."order_size" AS "order_size"
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
              "order_id",
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
        ) AS R
          ON L."order_id" = R."order_id"
       """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_double_aggregation(global_graph, order_size_agg_by_cust_id_graph):
    """
    Test aggregating a non-time aware feature derived from ItemData
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
          SUM("order_size") AS sum_value_avg_05cfcb50dc74296f7dc96dc68d881404b1105e6e,
          COUNT("order_size") AS count_value_avg_05cfcb50dc74296f7dc96dc68d881404b1105e6e
        FROM (
            SELECT
              *,
              FLOOR((DATE_PART(EPOCH_SECOND, "ts") - DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP))) / 3600) AS tile_index
            FROM (
                SELECT
                  L."ts" AS "ts",
                  L."cust_id" AS "cust_id",
                  L."order_id" AS "order_id",
                  L."order_method" AS "order_method",
                  R."order_size" AS "order_size"
                FROM (
                    SELECT
                      *
                    FROM (
                        SELECT
                          "ts" AS "ts",
                          "cust_id" AS "cust_id",
                          "order_id" AS "order_id",
                          "order_method" AS "order_method"
                        FROM "db"."public"."event_table"
                    )
                    WHERE
                      "ts" >= CAST(__FB_START_DATE AS TIMESTAMP)
                      AND "ts" < CAST(__FB_END_DATE AS TIMESTAMP)
                ) AS L
                LEFT JOIN (
                    SELECT
                      "order_id",
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
                ) AS R
                  ON L."order_id" = R."order_id"
            )
        )
        GROUP BY
          tile_index,
          "cust_id"
        ORDER BY
          tile_index
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected
