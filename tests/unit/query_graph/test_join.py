"""
Tests for Join SQLNode
"""

import pytest
from pydantic import ValidationError

from featurebyte.query_graph.enum import NodeOutputType, NodeType
from featurebyte.query_graph.node.generic import JoinNode
from featurebyte.query_graph.sql.builder import SQLOperationGraph
from featurebyte.query_graph.sql.common import SQLType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(name="item_table_join_event_table_filtered_node")
def item_table_join_event_table_filtered_node_fixture(
    global_graph, item_table_join_event_table_node
):
    """
    Apply filtering on a join node
    """
    graph = global_graph
    proj_a = graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["item_type"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[item_table_join_event_table_node],
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
        input_nodes=[item_table_join_event_table_node, condition],
    )
    return filtered_node


@pytest.fixture
def derived_expression_from_join_node(global_graph, item_table_join_event_table_node):
    """
    ExpressionNode derived from a join node
    """
    project_node = global_graph.add_operation(
        node_type=NodeType.PROJECT,
        node_params={"columns": ["item_id"]},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[global_graph.get_node_by_name(item_table_join_event_table_node.name)],
    )
    add_node = global_graph.add_operation(
        node_type=NodeType.ADD,
        node_params={"value": 123},
        node_output_type=NodeOutputType.SERIES,
        input_nodes=[project_node],
    )
    return add_node


def test_item_table_join_event_table_attributes(
    global_graph, item_table_join_event_table_node, source_info, update_fixtures
):
    """
    Test SQL generation for ItemTable joined with EventTable
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.MATERIALIZE, source_info=source_info
    )
    sql_tree = sql_graph.build(item_table_join_event_table_node).sql
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_item_table_join_event_table_attributes.sql",
        update_fixtures,
    )


def test_item_table_join_event_table_attributes_with_filter(
    global_graph, item_table_join_event_table_filtered_node, source_info, update_fixtures
):
    """
    Test SQL generation for ItemTable joined with EventTable with filter
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.MATERIALIZE, source_info=source_info
    )
    sql_tree = sql_graph.build(item_table_join_event_table_filtered_node).sql
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_item_table_join_event_table_attributes_with_filter.sql",
        update_fixtures,
    )


def test_item_table_join_event_table_attributes_on_demand_tile_gen(
    global_graph, item_table_joined_event_table_feature_node, source_info, update_fixtures
):
    """
    Test on-demand tile SQL generation for ItemView
    """
    interpreter = GraphInterpreter(global_graph, source_info)
    groupby_node_name = global_graph.get_input_node_names(
        item_table_joined_event_table_feature_node
    )[0]
    groupby_node = global_graph.get_node_by_name(groupby_node_name)
    tile_gen_sqls = interpreter.construct_tile_gen_sql(groupby_node, is_on_demand=True)
    assert len(tile_gen_sqls) == 1
    assert_equal_with_expected_fixture(
        tile_gen_sqls[0].sql,
        "tests/fixtures/query_graph/test_join/test_item_table_join_event_table_attributes_on_demand_tile_gen.sql",
        update_fixtures,
    )


def test_item_groupby_feature_joined_event_view(
    global_graph, order_size_feature_join_node, source_info, update_fixtures
):
    """
    Test SQL generation for non-time aware feature in ItemTable joined into EventView
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.MATERIALIZE, source_info=source_info
    )
    sql_tree = sql_graph.build(order_size_feature_join_node).sql
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_item_groupby_feature_joined_event_view.sql",
        update_fixtures,
    )


def test_derived_expression_from_join_node(
    global_graph, derived_expression_from_join_node, source_info, update_fixtures
):
    """
    Test derived expression from join node
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.MATERIALIZE, source_info=source_info
    )
    sql_tree = sql_graph.build(derived_expression_from_join_node).sql_standalone
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_derived_expression_from_join_node.sql",
        update_fixtures,
    )


def test_double_aggregation(
    global_graph, order_size_agg_by_cust_id_graph, source_info, update_fixtures
):
    """
    Test aggregating a non-time aware feature derived from ItemTable
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.BUILD_TILE, source_info=source_info
    )
    _, node = order_size_agg_by_cust_id_graph
    sql_tree = (
        sql_graph.build(node)
        .get_tile_compute_spec()
        .get_tile_compute_query()
        .get_combined_query_expr()
    )
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_double_aggregation.sql",
        update_fixtures,
    )


def test_scd_join(global_graph, scd_join_node, source_info, update_fixtures):
    """
    Test SQL generation for SCD join
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.MATERIALIZE, source_info=source_info
    )
    sql_tree = sql_graph.build(scd_join_node).sql
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_scd_join.sql",
        update_fixtures,
    )


@pytest.mark.parametrize(
    "param_name",
    ["left_input_columns", "left_output_columns", "right_input_columns", "right_output_columns"],
)
def test_join_node_parameters_validation__duplicated_columns(param_name, join_node_params):
    """Test join node parameter validation logic"""
    with pytest.raises(ValidationError) as exc:
        JoinNode(name="join1", parameters={**join_node_params, param_name: ["a", "a"]})
    expected_msg = "Column names (values: ['a', 'a']) must be unique!"
    assert expected_msg in str(exc.value)


def test_join_node_parameters_validation__overlapped_columns(join_node_params):
    """Test join node parameter validation logic"""
    with pytest.raises(ValidationError) as exc:
        JoinNode(
            name="join1",
            parameters={
                **join_node_params,
                "right_output_columns": join_node_params["left_output_columns"],
            },
        )
    expected_msg = "Left and right output columns should not have common item(s)."
    assert expected_msg in str(exc.value)


def test_event_table_join_snapshots_table(
    global_graph, event_table_join_snapshots_table_node, source_info, update_fixtures
):
    """
    Test SQL generation for EventTable joined with SnapshotsTable
    """
    sql_graph = SQLOperationGraph(
        global_graph, sql_type=SQLType.MATERIALIZE, source_info=source_info
    )
    sql_tree = sql_graph.build(event_table_join_snapshots_table_node).sql
    assert_equal_with_expected_fixture(
        sql_tree.sql(pretty=True),
        "tests/fixtures/query_graph/test_join/test_event_table_join_snapshots_table.sql",
        update_fixtures,
    )
