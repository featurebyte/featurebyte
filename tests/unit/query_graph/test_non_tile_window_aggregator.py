"""
Tests for NonTileWindowAggregator
"""

import pytest
from sqlglot.expressions import select

from featurebyte import SourceType
from featurebyte.query_graph.sql.aggregator.non_tile_window import NonTileWindowAggregator
from featurebyte.query_graph.sql.common import construct_cte_sql
from featurebyte.query_graph.sql.specifications.non_tile_window_aggregate import (
    NonTileWindowAggregateSpec,
)
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture
def agg_specs(global_graph, non_tile_window_aggregate_feature_node):
    """
    Fixture of TileAggregationSpec without window
    """
    parent_nodes = global_graph.get_input_node_names(non_tile_window_aggregate_feature_node)
    assert len(parent_nodes) == 1
    agg_node = global_graph.get_node_by_name(parent_nodes[0])
    return NonTileWindowAggregateSpec.from_query_graph_node(
        node=agg_node,
        graph=global_graph,
        source_type=SourceType.SNOWFLAKE,
        agg_result_name_include_serving_names=True,
    )


def test_non_tile_window_aggregate(agg_specs, update_fixtures):
    """
    Test non tile window aggregator
    """
    aggregator = NonTileWindowAggregator(source_type=SourceType.SNOWFLAKE)
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
        construct_cte_sql(aggregator.get_common_table_expressions("REQUEST_TABLE")).select(1).sql(pretty=True),
        "tests/fixtures/aggregator/expected_non_tile_window_aggregator_ctes.sql",
        update_fixture=update_fixtures,
    )
