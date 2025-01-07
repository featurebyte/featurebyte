"""
Tests for TimeSeriesWindowAggregator
"""

import pytest

from featurebyte.query_graph.model.window import CalendarWindow
from featurebyte.query_graph.sql.aggregator.time_series_window import TimeSeriesWindowAggregator
from featurebyte.query_graph.sql.specifications.time_series_window_aggregate import (
    TimeSeriesWindowAggregateSpec,
)
from tests.unit.query_graph.util import get_combined_aggregation_expr_from_aggregator
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(name="base_agg_spec")
def base_agg_spec_fixture(global_graph, time_series_window_aggregate_feature_node, source_info):
    """
    Fixture of TimeSeriesAggregateSpec
    """
    parent_nodes = global_graph.get_input_node_names(time_series_window_aggregate_feature_node)
    assert len(parent_nodes) == 1
    agg_node = global_graph.get_node_by_name(parent_nodes[0])
    return TimeSeriesWindowAggregateSpec.from_query_graph_node(
        node=agg_node,
        graph=global_graph,
        source_info=source_info,
        agg_result_name_include_serving_names=True,
    )[0]


@pytest.fixture(name="day_window_agg_spec")
def day_window_agg_spec_fixture(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with day based window
    """
    base_agg_spec.window = CalendarWindow(unit="DAY", size=7)
    return base_agg_spec


@pytest.fixture(name="month_window_agg_spec")
def month_window_agg_spec_fixture(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with month based window
    """
    base_agg_spec.window = CalendarWindow(unit="MONTH", size=3)
    return base_agg_spec


@pytest.fixture(name="day_with_offset_agg_spec")
def day_with_offset_agg_spec(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with day based window
    """
    base_agg_spec.window = CalendarWindow(unit="DAY", size=7)
    base_agg_spec.offset = CalendarWindow(unit="DAY", size=3)
    return base_agg_spec


@pytest.fixture(name="month_with_offset_agg_spec")
def month_with_offset_agg_spec(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with month based window
    """
    base_agg_spec.window = CalendarWindow(unit="MONTH", size=3)
    base_agg_spec.offset = CalendarWindow(unit="MONTH", size=1)
    return base_agg_spec


@pytest.mark.parametrize("test_case_name", ["day", "month", "day_with_offset", "month_with_offset"])
def test_aggregator(request, test_case_name, update_fixtures, source_info):
    """
    Test time series window aggregator for a daily window
    """
    test_case_mapping = {
        "day": "day_window_agg_spec",
        "month": "month_window_agg_spec",
        "day_with_offset": "day_with_offset_agg_spec",
        "month_with_offset": "month_with_offset_agg_spec",
    }
    fixture_name = test_case_mapping[test_case_name]
    agg_spec = request.getfixturevalue(fixture_name)

    aggregator = TimeSeriesWindowAggregator(source_info=source_info)
    aggregator.update(agg_spec)
    result_expr = get_combined_aggregation_expr_from_aggregator(aggregator)
    assert_equal_with_expected_fixture(
        result_expr.sql(pretty=True),
        f"tests/fixtures/aggregator/expected_non_tile_window_aggregator_{test_case_name}.sql",
        update_fixture=update_fixtures,
    )
