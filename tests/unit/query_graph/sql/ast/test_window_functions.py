import textwrap

import pytest

from featurebyte import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType


@pytest.fixture
def graph_with_window_function_filter(global_graph, input_node):
    graph = global_graph
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
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
        input_nodes=[input_node, non_window_based_condition],
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
    yield graph, filtered_node_2


def test_window_function__as_filter_qualify_not_supported(graph_with_window_function_filter):
    """
    Test window function as filter but QUALIFY is not supported
    """
    graph, node = graph_with_window_function_filter
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SPARK
    )
    sql_tree = sql_graph.build(node).sql
    expected = textwrap.dedent(
        """
        SELECT
          "ts",
          "cust_id",
          "a",
          "b",
          "prev_a"
        FROM (
          SELECT
            "ts" AS "ts",
            "cust_id" AS "cust_id",
            "a" AS "a",
            "b" AS "b",
            LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) AS "prev_a",
            (
              LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
            ) AS "__fb_qualify_condition_column"
          FROM "db"."public"."event_table"
          WHERE
            (
              "a" = 123
            )
        )
        WHERE
          "__fb_qualify_condition_column"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected


def test_window_function__as_filter_qualify_not_supported_unnamed(
    graph_with_window_function_filter,
):
    """
    Test window function as filter but QUALIFY is not supported. Project a temporary expression.
    """
    graph, node = graph_with_window_function_filter
    project_node = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[node],
    )
    add_node = graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node],
    )
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.EVENT_VIEW_PREVIEW, source_type=SourceType.SPARK
    )
    sql_tree = sql_graph.build(add_node).sql_standalone
    expected = textwrap.dedent(
        """
        SELECT
          "Unnamed0"
        FROM (
          SELECT
            (
              "a" + 123
            ) AS "Unnamed0",
            (
              LAG("a", 1) OVER (PARTITION BY "cust_id" ORDER BY "ts" NULLS LAST) > 0
            ) AS "__fb_qualify_condition_column"
          FROM "db"."public"."event_table"
          WHERE
            (
              "a" = 123
            )
        )
        WHERE
          "__fb_qualify_condition_column"
        """
    ).strip()
    assert sql_tree.sql(pretty=True) == expected
