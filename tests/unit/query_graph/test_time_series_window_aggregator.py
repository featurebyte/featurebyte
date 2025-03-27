"""
Tests for TimeSeriesWindowAggregator
"""

import copy

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


@pytest.fixture(name="latest_agg_spec")
def latest_agg_spec_fixture(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with latest method
    """
    base_agg_spec.window = CalendarWindow(unit="DAY", size=7)
    base_agg_spec.parameters.agg_func = "latest"
    return base_agg_spec


@pytest.fixture(name="mixed_order_dependency_agg_specs")
def mixed_order_dependency_specs_fixture(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with mixed order dependent and order independent aggregation
    """
    agg_spec_1 = copy.deepcopy(base_agg_spec)
    agg_spec_1.parameters.agg_func = "latest"
    agg_spec_1.parameters.names = ["feature_latest"]

    agg_spec_2 = copy.deepcopy(base_agg_spec)
    agg_spec_2.parameters.agg_func = "sum"
    agg_spec_2.parameters.names = ["feature_sum"]

    return [agg_spec_1, agg_spec_2]


@pytest.fixture(name="reference_datetime_is_utc_time_agg_spec")
def reference_datetime_is_utc_agg_spec_fixture(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec with reference datetime is UTC
    """
    base_agg_spec.parameters.reference_datetime_schema.is_utc_time = True
    return base_agg_spec


@pytest.fixture(name="grouped_agg_specs")
def grouped_agg_specs_fixture(base_agg_spec):
    """
    Fixture of TimeSeriesAggregateSpec objects that can be grouped in sql query
    """
    agg_spec_1 = copy.deepcopy(base_agg_spec)
    agg_spec_1.parameters.agg_func = "sum"
    agg_spec_1.parameters.names = ["feature_sum"]

    agg_spec_2 = copy.deepcopy(base_agg_spec)
    agg_spec_2.parameters.agg_func = "min"
    agg_spec_2.parameters.names = ["feature_min"]

    agg_spec_3 = copy.deepcopy(base_agg_spec)
    agg_spec_3.parameters.agg_func = "max"
    agg_spec_3.parameters.names = ["feature_ax"]

    return [agg_spec_1, agg_spec_2, agg_spec_3]


@pytest.mark.parametrize(
    "test_case_name",
    [
        "day",
        "month",
        "day_with_offset",
        "month_with_offset",
        "latest",
        "mixed_order_dependency",
        "is_utc_time",
        "grouped",
    ],
)
def test_aggregator(request, test_case_name, update_fixtures, source_info):
    """
    Test time series window aggregator for a daily window
    """
    test_case_mapping = {
        "day": "day_window_agg_spec",
        "month": "month_window_agg_spec",
        "day_with_offset": "day_with_offset_agg_spec",
        "month_with_offset": "month_with_offset_agg_spec",
        "latest": "latest_agg_spec",
        "mixed_order_dependency": "mixed_order_dependency_agg_specs",
        "is_utc_time": "reference_datetime_is_utc_time_agg_spec",
        "grouped": "grouped_agg_specs",
    }
    fixture_name = test_case_mapping[test_case_name]
    fixture_obj = request.getfixturevalue(fixture_name)
    if isinstance(fixture_obj, list):
        agg_specs = fixture_obj
    else:
        agg_specs = [fixture_obj]

    aggregator = TimeSeriesWindowAggregator(source_info=source_info)
    for agg_spec in agg_specs:
        aggregator.update(agg_spec)
    result_expr = get_combined_aggregation_expr_from_aggregator(aggregator)
    assert_equal_with_expected_fixture(
        result_expr.sql(pretty=True),
        f"tests/fixtures/aggregator/expected_time_series_window_aggregator_{test_case_name}.sql",
        update_fixture=update_fixtures,
    )
