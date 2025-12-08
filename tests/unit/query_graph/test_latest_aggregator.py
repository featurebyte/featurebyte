"""
Unit tests for featurebyte.query_graph.sql.aggregator.latest.LatestAggregator

"""

import pytest
from sqlglot.expressions import select

from featurebyte.query_graph.sql.aggregator.latest import LatestAggregator
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture
def agg_specs_no_window(
    global_graph, latest_value_without_window_feature_node, adapter, source_info
):
    """
    Fixture of TileAggregationSpec without window
    """
    parent_nodes = global_graph.get_input_node_names(latest_value_without_window_feature_node)
    assert len(parent_nodes) == 1
    groupby_node = global_graph.get_node_by_name(parent_nodes[0])
    return TileBasedAggregationSpec.from_query_graph_node(
        node=groupby_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )


@pytest.fixture
def agg_specs_offset(
    global_graph, latest_value_offset_without_window_feature_node, adapter, source_info
):
    """
    Fixture of TileAggregationSpec with unbounded window and offset
    """
    parent_nodes = global_graph.get_input_node_names(
        latest_value_offset_without_window_feature_node
    )
    assert len(parent_nodes) == 1
    groupby_node = global_graph.get_node_by_name(parent_nodes[0])
    return TileBasedAggregationSpec.from_query_graph_node(
        node=groupby_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )


def create_latest_aggregator(agg_specs, source_info, **kwargs):
    """
    Helper function to create a LatestAggregator
    """
    aggregator = LatestAggregator(source_info=source_info, **kwargs)
    for spec in agg_specs:
        aggregator.update(spec)
    return aggregator


def test_get_required_serving_names(agg_specs_no_window, source_info):
    """
    Test get_required_serving_names method
    """
    aggregator = LatestAggregator(source_info=source_info)
    for spec in agg_specs_no_window:
        aggregator.update(spec)
    assert aggregator.get_required_serving_names() == {"CUSTOMER_ID", "BUSINESS_ID"}


def test_latest_aggregator(agg_specs_no_window, source_info, update_fixtures):
    """
    Test LatestAggregator update_aggregation_table_expr
    """
    aggregator = create_latest_aggregator(agg_specs_no_window, source_info=source_info)
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 1

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert_equal_with_expected_fixture(
        result.updated_table_expr.sql(pretty=True),
        "tests/fixtures/aggregator/expected_latest_aggregator.sql",
        update_fixture=update_fixtures,
    )

    assert result.column_names == [
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
    ]
    assert result.updated_index == 0


def test_latest_aggregator__no_specs(source_info):
    """
    Test calling update_aggregation_table_expr when there are no unbounded windows (no specs)
    """
    aggregator = create_latest_aggregator([], source_info=source_info)
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 0

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert result.updated_table_expr.sql() == "SELECT a, b, c FROM my_table"
    assert result.column_names == []
    assert result.updated_index == 0


def test_latest_aggregator__online_retrieval(agg_specs_no_window, source_info, update_fixtures):
    """
    Test feature values should be calculated on demand from the tile table during online retrieval
    """
    aggregator = create_latest_aggregator(
        agg_specs_no_window, source_info=source_info, is_online_serving=True
    )
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 1

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert_equal_with_expected_fixture(
        result.updated_table_expr.sql(pretty=True),
        "tests/fixtures/aggregator/expected_latest_aggregator_update_online.sql",
        update_fixture=update_fixtures,
    )
    assert result.column_names == [
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78"
    ]
    assert result.updated_index == 0


def test_latest_aggregator_offset(agg_specs_offset, source_info, update_fixtures):
    """
    Test latest aggregation with offset
    """
    aggregator = create_latest_aggregator(
        agg_specs_offset, source_info=source_info, is_online_serving=True
    )
    assert len(list(aggregator.specs_set.get_grouped_aggregation_specs())) == 1

    result = aggregator.update_aggregation_table_expr(
        table_expr=select("a", "b", "c").from_("my_table"),
        point_in_time_column="POINT_IN_TIME",
        current_columns=["a", "b", "c"],
        current_query_index=0,
    )
    assert_equal_with_expected_fixture(
        result.updated_table_expr.sql(pretty=True),
        "tests/fixtures/aggregator/expected_latest_aggregator_update_offset.sql",
        update_fixture=update_fixtures,
    )
    assert result.column_names == [
        "_fb_internal_CUSTOMER_ID_BUSINESS_ID_latest_5d7c2d9af3442ddd52e26b3603f39aa922965d78_o172800",
    ]
    assert result.updated_index == 0
