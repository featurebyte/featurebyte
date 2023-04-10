import textwrap

import pytest

from featurebyte import SourceType
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from featurebyte.query_graph.util import get_aggregation_identifier, get_tile_table_identifier


def make_lag_node(graph, input_node, column_name, entity_column_name, timestamp_column_name):
    """
    Helper function to create a LagNode in the query graph
    """
    proj_column = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [column_name]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    proj_entity = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [entity_column_name]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    proj_ts = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": [timestamp_column_name]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
    )
    lagged_node = graph.add_operation(
        node_type=NodeType.LAG,
        node_params={
            "timestamp_column": timestamp_column_name,
            "entity_columns": [entity_column_name],
            "offset": 1,
        },
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[proj_column, proj_entity, proj_ts],
    )
    return lagged_node


@pytest.fixture
def graph_with_window_function_filter(global_graph, input_node):
    """
    Fixture with a graph where a window function is used as filter
    """
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
    lagged_a = make_lag_node(graph, filtered_node, "a", "cust_id", "ts")
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


def test_window_function(global_graph, input_node):
    """Test tile sql when window function is involved

    Note that the tile start and end date filters are applied on a nested subquery containing the
    window expression, not on the same select statement. This is so that the table required by
    the window expression is not filtered prematurely.
    """
    graph = global_graph
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[input_node],
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
        input_nodes=[input_node, binary_node],
    )
    lagged_a = make_lag_node(graph, filtered_input_node, "a", "cust_id", "ts")
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
          COUNT(*) AS value_count_bdd430697527efc219056ed856d0fc44ad2388ed
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


def test_window_function__as_filter(global_graph, input_node):
    """Test when condition derived from window function is used as filter"""
    graph = global_graph
    lagged_a = make_lag_node(graph, input_node, "a", "cust_id", "ts")
    assign_node = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "prev_a"},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[input_node, lagged_a],
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
        graph, sql_type=SQLType.MATERIALISE, source_type=SourceType.SNOWFLAKE
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


def test_window_function__multiple_filters(graph_with_window_function_filter):
    """Test when condition derived from window function is used as filter"""
    graph, filtered_node_2 = graph_with_window_function_filter
    sql_graph = SQLOperationGraph(
        graph, sql_type=SQLType.MATERIALISE, source_type=SourceType.SNOWFLAKE
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


def test_window_function__as_filter_qualify_not_supported(graph_with_window_function_filter):
    """
    Test window function as filter but QUALIFY is not supported
    """
    graph, node = graph_with_window_function_filter
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.MATERIALISE, source_type=SourceType.SPARK)
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
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.MATERIALISE, source_type=SourceType.SPARK)
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
