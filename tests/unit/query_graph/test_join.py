import copy
import textwrap

import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from tests.util.helper import add_groupby_operation


@pytest.fixture(name="item_data_input_details")
def item_data_input_details_fixture(input_details):
    input_details = copy.deepcopy(input_details)
    input_details["table_details"]["table_name"] = "item_table"
    return input_details


@pytest.fixture(name="item_data_input_node")
def item_data_input_node_fixture(global_graph, item_data_input_details):
    node_params = {
        "type": "item_data",
        "columns": ["order_id", "item_id", "item_name", "item_type"],
    }
    node_params.update(item_data_input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="event_data_input_node")
def event_data_input_node_fixture(global_graph, input_details):
    """Fixture of a query with some operations ready to run groupby"""
    # pylint: disable=duplicate-code
    node_params = {
        "type": "event_data",
        "columns": ["ts", "cust_id", "order_id", "order_method"],
        "timestamp": "ts",
    }
    node_params.update(input_details)
    node_input = global_graph.add_operation(
        node_type=NodeType.INPUT,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[],
    )
    return node_input


@pytest.fixture(name="item_data_join_event_data_node")
def item_data_join_event_data_node_fixture(
    global_graph,
    item_data_input_node,
    event_data_input_node,
):
    """
    Result of:

    item_view.join_event_data_attributes()
    """
    node_params = {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["order_id", "item_id", "item_name", "item_type"],
        "left_output_columns": ["order_id", "item_id", "item_name", "item_type"],
        "right_input_columns": ["order_method"],
        "right_output_columns": ["order_method"],
        "join_type": "left",
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_data_input_node, event_data_input_node],
    )
    return node


@pytest.fixture(name="order_size_feature_group_node")
def order_size_feature_group_node_fixture(global_graph, item_data_input_node):
    """
    Result of:

    item_view.groupby("order_id").aggregate(method="count")
    """
    node_params = {
        "keys": ["order_id"],
        "serving_names": ["order_id"],
        "parent": None,
        "agg_func": "count",
        "names": ["order_size"],
    }
    groupby_node = global_graph.add_operation(
        node_type=NodeType.ITEM_GROUPBY,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[item_data_input_node],
    )
    return groupby_node


@pytest.fixture(name="order_size_feature_join_node")
def order_size_feature_join_node_fixture(
    global_graph,
    order_size_feature_group_node,
    event_data_input_node,
):
    """
    Result of:

    event_view["order_size"] = order_size_feature.get_value(entity="order_id")
    """
    node_params = {
        "left_on": "order_id",
        "right_on": "order_id",
        "left_input_columns": ["ts", "cust_id", "order_id", "order_method"],
        "left_output_columns": ["ts", "cust_id", "order_id", "order_method"],
        "right_input_columns": ["order_size"],
        "right_output_columns": ["order_size"],
        "join_type": "left",
    }
    node = global_graph.add_operation(
        node_type=NodeType.JOIN,
        node_params=node_params,
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[event_data_input_node, order_size_feature_group_node],
    )
    return node


@pytest.fixture(name="order_size_agg_by_cust_id_node")
def order_size_agg_by_cust_id_node_fixture(global_graph, order_size_feature_join_node):
    node_params = {
        "keys": ["cust_id"],
        "serving_names": ["CUSTOMER_ID"],
        "value_by": None,
        "parent": "order_size",
        "agg_func": "avg",
        "time_modulo_frequency": 1800,  # 30m
        "frequency": 3600,  # 1h
        "blind_spot": 900,  # 15m
        "timestamp": "ts",
        "names": ["order_size_30d_avg"],
        "windows": ["30d"],
    }
    node = add_groupby_operation(global_graph, node_params, order_size_feature_join_node)
    return node


def test_item_data_join_event_data_attributes(global_graph, item_data_join_event_data_node):
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(item_data_join_event_data_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          L."order_id" AS "order_id",
          L."item_id" AS "item_id",
          L."item_name" AS "item_name",
          L."item_type" AS "item_type",
          R."order_method" AS "order_method"
        FROM (
            SELECT
              "order_id" AS "order_id",
              "item_id" AS "item_id",
              "item_name" AS "item_name",
              "item_type" AS "item_type"
            FROM "db"."public"."item_table"
        ) AS L
        LEFT JOIN (
            SELECT
              "ts" AS "ts",
              "cust_id" AS "cust_id",
              "order_id" AS "order_id",
              "order_method" AS "order_method"
            FROM "db"."public"."event_table"
        ) AS R
          ON L."order_id" = R."order_id"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_order_size_feature(global_graph, order_size_feature_join_node):
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


def test_double_aggregation(global_graph, order_size_agg_by_cust_id_node):
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.BUILD_TILE, source_type=SourceType.SNOWFLAKE
    )
    sql_tree = sql_graph.build(order_size_agg_by_cust_id_node).sql
    expected = textwrap.dedent(
        """
        SELECT
          TO_TIMESTAMP(DATE_PART(EPOCH_SECOND, CAST(__FB_START_DATE AS TIMESTAMP)) + tile_index * 3600) AS __FB_TILE_START_DATE_COLUMN,
          "cust_id",
          SUM("order_size") AS sum_value_avg_73fd02fb35339e2c6c1c9d66494b6d7829fbcb26,
          COUNT("order_size") AS count_value_avg_73fd02fb35339e2c6c1c9d66494b6d7829fbcb26
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
