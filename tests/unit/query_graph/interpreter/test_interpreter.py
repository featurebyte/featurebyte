"""
This module contains the tests for the Query Graph Interpreter
"""

import json
import os
import textwrap
from dataclasses import asdict

import pandas as pd
import pytest

from featurebyte.enum import InternalName
from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.util.helper import assert_equal_with_expected_fixture


def test_graph_interpreter_super_simple(simple_graph, source_info):
    """Test using a simple query graph"""
    graph, node = simple_graph
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.MATERIALIZE, source_info=source_info)
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


def test_graph_interpreter_assign_scalar(graph, node_input, source_info):
    """Test using a simple query graph"""
    assign = graph.add_operation(
        node_type=NodeType.ASSIGN,
        node_params={"name": "x", "value": 123},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.MATERIALIZE, source_info=source_info)
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


def test_graph_interpreter_multi_assign(graph, node_input, source_info):
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
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.BUILD_TILE, source_info=source_info)
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
def test_graph_interpreter_binary_operations(
    graph, node_input, node_type, expected_expr, source_info
):
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
    sql_graph = SQLOperationGraph(graph, SQLType.BUILD_TILE, source_info=source_info)
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


def test_graph_interpreter_project_multiple_columns(graph, node_input, source_info):
    """Test using a simple query graph"""
    proj = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["a", "b"]},
        node_output_type=NodeOutputType.FRAME,
        input_nodes=[node_input],
    )
    sql_graph = SQLOperationGraph(graph, sql_type=SQLType.MATERIALIZE, source_info=source_info)
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


def test_graph_interpreter_tile_gen(
    query_graph_with_groupby, groupby_node_aggregation_id, source_info
):
    """Test tile building SQL"""
    interpreter = GraphInterpreter(query_graph_with_groupby, source_info)
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "tile_id_version": 1,
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
        "tile_value_types": ["FLOAT", "FLOAT"],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "offset": None,
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": None,
        "parent": "a",
        "agg_func": "avg",
    }


def test_graph_interpreter_on_demand_tile_gen(
    query_graph_with_groupby, groupby_node_aggregation_id, source_info, update_fixtures
):
    """Test tile building SQL with on-demand tile generation

    Note that the input table query contains a inner-join with an entity table to filter only table
    belonging to specific entity IDs and date range
    """
    interpreter = GraphInterpreter(query_graph_with_groupby, source_info)
    groupby_node = query_graph_with_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand.sql",
        update_fixtures,
    )
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "tile_id_version": 1,
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
        "tile_value_types": ["FLOAT", "FLOAT"],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "offset": None,
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": None,
        "parent": "a",
        "agg_func": "avg",
    }


def test_graph_interpreter_tile_gen_with_category(
    query_graph_with_category_groupby, source_info, update_fixtures
):
    """Test tile building SQL with aggregation per category"""
    interpreter = GraphInterpreter(query_graph_with_category_groupby, source_info)
    groupby_node = query_graph_with_category_groupby.get_node_by_name("groupby_1")
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=False)
    assert len(tile_gen_sqls) == 1

    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")

    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand_with_category.sql",
        update_fixtures,
    )
    aggregation_id = "dfee6d136c6d6db110606afd33ae34deb9b5e96f"
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226",
        "tile_id_version": 1,
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
        "tile_value_types": ["FLOAT", "FLOAT"],
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "offset": None,
        "serving_names": ["CUSTOMER_ID"],
        "value_by_column": "product_type",
        "parent": "a",
        "agg_func": "avg",
    }


def test_graph_interpreter_on_demand_tile_gen_two_groupby(
    complex_feature_query_graph, groupby_node_aggregation_id, source_info, update_fixtures
):
    """Test case for a complex feature that depends on two groupby nodes"""
    complex_feature_node, graph = complex_feature_query_graph
    interpreter = GraphInterpreter(graph, source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(complex_feature_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 2

    # Check required tile 1 (groupby keys: cust_id)
    info = tile_gen_sqls[0]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "tile_id_version": 1,
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
        "tile_value_types": ["FLOAT", "FLOAT"],
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["2h", "48h"],
        "offset": None,
        "parent": "a",
        "agg_func": "avg",
    }
    assert_equal_with_expected_fixture(
        info.sql,
        "tests/fixtures/expected_tile_sql_on_demand_two_groupby_1.sql",
        update_fixtures,
    )

    # Check required tile 2 (groupby keys: biz_id)
    aggregation_id = "8c11e770ad5121aec588693662ac607b4fba0528"
    info = tile_gen_sqls[1]
    info_dict = asdict(info)
    info_dict.pop("tile_compute_spec")
    assert info_dict == {
        "tile_table_id": "TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624",
        "tile_id_version": 1,
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
        "tile_value_types": ["FLOAT"],
        "time_modulo_frequency": 1800,
        "frequency": 3600,
        "blind_spot": 900,
        "windows": ["7d"],
        "offset": None,
        "parent": "a",
        "agg_func": "sum",
    }
    assert_equal_with_expected_fixture(
        info.sql,
        "tests/fixtures/expected_tile_sql_on_demand_two_groupby_2.sql",
        update_fixtures,
    )


def test_one_demand_tile_gen_on_simple_view(
    global_graph, window_aggregate_on_simple_view_feature_node, update_fixtures, source_info
):
    """Test tile building SQL with on-demand tile generation on a simple view

    No additional inner joins should be applied
    """
    interpreter = GraphInterpreter(global_graph, source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(
        window_aggregate_on_simple_view_feature_node, is_on_demand=True
    )
    assert len(tile_gen_sqls) == 1
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand_simple.sql",
        update_fixtures,
    )


def test_on_demand_tile_gen_on_joined_view(
    global_graph, window_aggregate_on_view_with_scd_join_feature_node, update_fixtures, source_info
):
    """Test tile building SQL with on-demand tile generation on a joined view

    Input tables should be filtered using a join with the entity table before performing the more
    expensive SCD join.
    """
    interpreter = GraphInterpreter(global_graph, source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(
        window_aggregate_on_view_with_scd_join_feature_node, is_on_demand=True
    )
    assert len(tile_gen_sqls) == 1
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand_joined_view.sql",
        update_fixtures,
    )


def test_on_demand_tile_gen_on_joined_view_complex_composite_keys(
    global_graph,
    complex_composite_window_aggregate_on_view_with_scd_join_feature_node,
    source_info,
    update_fixtures,
):
    """Test tile building SQL with on-demand tile generation on a joined view using composite keys
    from different tables. For now, filter is only supported on the non-derived entity column.
    """
    interpreter = GraphInterpreter(global_graph, source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(
        complex_composite_window_aggregate_on_view_with_scd_join_feature_node, is_on_demand=True
    )
    assert len(tile_gen_sqls) == 1
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_on_demand_complex_composite.sql",
        update_fixtures,
    )


def test_graph_interpreter_preview(graph, node_input, source_info):
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
    interpreter = GraphInterpreter(graph, source_info)

    sql_code = interpreter.construct_preview_sql("assign_2")[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
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


def test_filter_node(graph, node_input, source_info):
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
    interpreter = GraphInterpreter(graph, source_info)
    sql_code = interpreter.construct_preview_sql(filter_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
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

    interpreter = GraphInterpreter(graph, source_info)
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


def test_multiple_filters(graph, node_input, source_info):
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
    interpreter = GraphInterpreter(graph, source_info)
    sql_code = interpreter.construct_preview_sql(filter_node_2.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
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


def test_filter_assign_project(graph, node_input, source_info):
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
    interpreter = GraphInterpreter(graph, source_info)
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


def test_project_multi_then_assign(graph, node_input, source_info):
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
    interpreter = GraphInterpreter(graph, source_info)
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


def test_conditional_assign__project_named(graph, node_input, source_info):
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

    interpreter = GraphInterpreter(graph, source_info)
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

    interpreter = GraphInterpreter(graph, source_info)
    sql_code = interpreter.construct_preview_sql(assign_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          "ts" AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
          CASE WHEN (
            "a" = -999
          ) THEN 0 ELSE "a" END AS "a",
          "b" AS "b"
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_isnull(graph, node_input, source_info):
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
    interpreter = GraphInterpreter(graph, source_info)
    sql_code = interpreter.construct_preview_sql(mask_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          (
            "a" IS NULL
          )
        FROM "db"."public"."event_table"
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_databricks_source(
    query_graph_with_groupby, groupby_node_aggregation_id, databricks_source_info, update_fixtures
):
    """Test SQL generation for databricks source"""
    graph = query_graph_with_groupby
    input_node = graph.get_node_by_name("input_1")
    groupby_node = graph.get_node_by_name("groupby_1")
    interpreter = GraphInterpreter(graph, source_info=databricks_source_info)

    # Check preview SQL
    preview_sql = interpreter.construct_preview_sql(input_node.name)[0]
    expected = textwrap.dedent(
        """
        SELECT
          `ts` AS `ts`,
          `cust_id` AS `cust_id`,
          `biz_id` AS `biz_id`,
          `product_type` AS `product_type`,
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
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/expected_tile_sql_databricks.sql",
        update_fixtures,
    )


def test_tile_sql_order_dependent_aggregation(
    global_graph, latest_value_aggregation_feature_node, source_info, update_fixtures
):
    """
    Test generating tile sql for an order dependent aggregation
    """
    interpreter = GraphInterpreter(global_graph, source_info=source_info)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(
        latest_value_aggregation_feature_node, is_on_demand=False
    )
    assert len(tile_gen_sqls) == 1
    tile_sql = tile_gen_sqls[0].sql
    assert_equal_with_expected_fixture(
        tile_sql,
        "tests/fixtures/expected_tile_sql_order_dependent_aggregation.sql",
        update_fixtures,
    )


def test_graph_interpreter_sample(simple_graph, source_info):
    """Test graph sample"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_info)

    sql_code = interpreter.construct_sample_sql(node.name, num_rows=10, seed=1234)[0]
    expected = textwrap.dedent(
        """
        SELECT
          IFF(
            CAST("ts" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
            OR CAST("ts" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
            NULL,
            "ts"
          ) AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
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


def test_graph_interpreter_sample_date_range(simple_graph, source_info):
    """Test graph sample with date range"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_info)

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
          IFF(
            CAST("ts" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
            OR CAST("ts" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
            NULL,
            "ts"
          ) AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          "a" AS "a_copy"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= CAST('2020-01-01T00:00:00' AS TIMESTAMP)
          AND "ts" < CAST('2020-01-03T00:00:00' AS TIMESTAMP)
        ORDER BY
          RANDOM(10)
        LIMIT 10
        """
    ).strip()
    assert sql_code == expected


def test_graph_interpreter_sample_date_range_no_timestamp_column(simple_graph, source_info):
    """Test graph sample with date range"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_info)

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
          IFF(
            CAST("ts" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
            OR CAST("ts" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
            NULL,
            "ts"
          ) AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
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


def test_graph_interpreter__construct_tile_gen_sql__item_join_dimension_join_scd_and_groupby(
    test_dir, source_info
):
    fixture_path = os.path.join(
        test_dir,
        "fixtures/graph/PRODUCT_vs_PRODUCTGROUP_item_TotalCost_across_customer_PostalCodes_28d.json",
    )
    with open(fixture_path, "r") as file_handle:
        graph_dict = json.load(file_handle)

    query_graph = QueryGraph(**graph_dict)
    node = query_graph.get_node_by_name("alias_1")
    interpreter = GraphInterpreter(query_graph, source_info)

    # check that the tile sql is generated without error
    tile_infos = interpreter.construct_tile_gen_sql(node, is_on_demand=False)
    assert len(tile_infos) == 2


@pytest.mark.parametrize("has_timestamp_schema", [True])
def test_graph_interpreter_sample_date_range_with_timestamp_schema(simple_graph, source_info):
    """Test graph sample with date range"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_info)

    sql_code = interpreter.construct_sample_sql(
        node.name,
        num_rows=30,
        seed=20,
        timestamp_column="ts",
        from_timestamp=pd.to_datetime("2020-01-01"),
        to_timestamp=pd.to_datetime("2020-01-03"),
    )[0]
    expected = textwrap.dedent(
        """
        SELECT
          CAST("ts" AS VARCHAR) AS "ts",
          CAST("cust_id" AS VARCHAR) AS "cust_id",
          "a" AS "a",
          "b" AS "b",
          "a" AS "a_copy"
        FROM "db"."public"."event_table"
        WHERE
          "ts" >= TO_CHAR(CAST('2020-01-01T00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
          AND "ts" < TO_CHAR(CAST('2020-01-03T00:00:00' AS TIMESTAMP), '%Y-%m-%d %H:%M:%S')
        ORDER BY
          RANDOM(20)
        LIMIT 30
        """
    ).strip()
    assert sql_code == expected
