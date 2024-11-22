"""
Tests for NonTileWindowAggregator
"""

import pytest
from sqlglot.expressions import select

from featurebyte.query_graph.sql.aggregator.non_tile_window import NonTileWindowAggregator
from featurebyte.query_graph.sql.common import construct_cte_sql, sql_to_string
from featurebyte.query_graph.sql.feature_historical import get_historical_features_expr
from featurebyte.query_graph.sql.specifications.non_tile_window_aggregate import (
    NonTileWindowAggregateSpec,
)
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture
def agg_specs(global_graph, non_tile_window_aggregate_feature_node, source_info):
    """
    Fixture of TileAggregationSpec without window
    """
    parent_nodes = global_graph.get_input_node_names(non_tile_window_aggregate_feature_node)
    assert len(parent_nodes) == 1
    agg_node = global_graph.get_node_by_name(parent_nodes[0])
    return NonTileWindowAggregateSpec.from_query_graph_node(
        node=agg_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )


def test_non_tile_window_aggregate(agg_specs, update_fixtures, source_info):
    """
    Test non tile window aggregator
    """
    aggregator = NonTileWindowAggregator(source_info=source_info)
    for spec in agg_specs:
        aggregator.update(spec)
    result = aggregator.update_aggregation_table_expr(
        select("POINT_IN_TIME", "cust_id").from_("REQUEST_TABLE"),
        "POINT_IN_TIME",
        ["POINT_IN_TIME", "cust_id"],
        0,
    )
    assert_equal_with_expected_fixture(
        result.updated_table_expr.sql(pretty=True),
        "tests/fixtures/aggregator/expected_non_tile_window_aggregator.sql",
        update_fixture=update_fixtures,
    )
    assert_equal_with_expected_fixture(
        construct_cte_sql(aggregator.get_common_table_expressions("REQUEST_TABLE"))
        .select(1)
        .sql(pretty=True),
        "tests/fixtures/aggregator/expected_non_tile_window_aggregator_ctes.sql",
        update_fixture=update_fixtures,
    )


def test_multiple_windows_complex_feature(
    global_graph, non_tile_window_aggregate_complex_feature_node, update_fixtures, source_info
):
    """
    Test non tile window aggregator with multiple windows
    """
    actual = sql_to_string(
        get_historical_features_expr(
            request_table_name="REQUEST_TABLE",
            graph=global_graph,
            nodes=[non_tile_window_aggregate_complex_feature_node],
            request_table_columns=["POINT_IN_TIME", "CUSTOMER_ID"],
            source_info=source_info,
        )[0],
        source_info.source_type,
    )
    assert_equal_with_expected_fixture(
        actual,
        "tests/fixtures/aggregator/expected_non_tile_window_complex_feature.sql",
        update_fixture=update_fixtures,
    )
