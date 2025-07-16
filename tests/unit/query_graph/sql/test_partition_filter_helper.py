"""
Tests for featurebyte/query_graph/sql/partition_filter_helper.py
"""

from datetime import datetime

import pytest

from featurebyte import TimeInterval
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.common import PartitionColumnFilters
from featurebyte.query_graph.sql.partition_filter_helper import get_partition_filters_from_graph


@pytest.fixture(name="input_node_has_id", autouse=True)
def input_node_has_id_fixture():
    """
    Override the default value for this fixture so that input_node has an ID (required to activate
    partition filters)
    """
    return True


@pytest.fixture(name="min_max_point_in_time")
def min_max_point_in_time_fixture():
    """
    Fixture to provide min and max point in time for testing
    """
    return (
        make_literal_value(datetime(2023, 1, 1, 0, 0, 0), cast_as_timestamp=True),
        make_literal_value(datetime(2023, 6, 1, 0, 0, 0), cast_as_timestamp=True),
    )


def check_partition_column_filters(
    partition_column_filters: PartitionColumnFilters,
    expected_mapping,
):
    """
    Helper function to check if partition column filters match the expected mapping. Automatically
    converts the expressions to string so that the expected mapping can be written in string.
    """
    mapping = partition_column_filters.mapping
    assert set(mapping.keys()) == set(expected_mapping.keys())
    converted_mapping = {
        table_id: {
            "from_timestamp": actual_filter.from_timestamp.sql(),
            "to_timestamp": actual_filter.to_timestamp.sql(),
            "buffer": actual_filter.buffer,
        }
        for table_id, actual_filter in mapping.items()
    }
    assert converted_mapping == expected_mapping


def test_tile_based_window_aggregate(
    global_graph,
    event_table_id,
    window_aggregate_on_view_with_scd_join_feature_node,
    min_max_point_in_time,
    adapter,
):
    """
    Test that partition filters are generated correctly for a tile-based window aggregate node
    """
    # A feature with window of 90d
    _ = window_aggregate_on_view_with_scd_join_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter=adapter,
    )
    expected_mapping = {
        event_table_id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -129600, 'MINUTE')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=1),
        }
    }
    check_partition_column_filters(partition_column_filters, expected_mapping)


def test_tile_based_window_aggregate_with_offset(
    global_graph,
    window_aggregate_with_offset_feature_node,
    event_table_id,
    min_max_point_in_time,
    adapter,
):
    """
    Test that partition filters are generated correctly for a tile-based window aggregate node with offset
    """
    # A feature with window of 24h and offset of 8h
    _ = window_aggregate_with_offset_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    expected_mapping = {
        event_table_id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -1920, 'MINUTE')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=1),
        }
    }
    check_partition_column_filters(partition_column_filters, expected_mapping)


def test_time_series_window_aggregate(
    global_graph,
    time_series_window_aggregate_feature_node,
    time_series_table_input_node,
    min_max_point_in_time,
    adapter,
):
    """
    Test that partition filters are generated correctly for a time series window aggregate node
    """
    # A feature with calendar window of 7d
    _ = time_series_window_aggregate_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    expected_mapping = {
        time_series_table_input_node.parameters.id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -10080, 'MINUTE')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=1),
        }
    }
    check_partition_column_filters(partition_column_filters, expected_mapping)


def test_time_series_window_aggregate_with_offset(
    global_graph,
    time_series_window_aggregate_with_offset_feature_node,
    time_series_table_input_node,
    min_max_point_in_time,
    adapter,
):
    """
    Test that partition filters are generated correctly for a time series window aggregate node
    """
    # A feature with calendar window of 7d and offset of 3d
    _ = time_series_window_aggregate_with_offset_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    expected_mapping = {
        time_series_table_input_node.parameters.id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -14400, 'MINUTE')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=1),
        }
    }
    check_partition_column_filters(partition_column_filters, expected_mapping)


@pytest.mark.parametrize("time_series_table_time_interval", [{"unit": "MONTH", "value": 1}])
def test_time_series_window_aggregate_monthly_interval(
    global_graph,
    time_series_window_aggregate_feature_node,
    time_series_table_input_node,
    min_max_point_in_time,
    adapter,
):
    """
    Test partition filters for a time series table with monthly interval
    """
    # A feature with calendar window of 7 month
    _ = time_series_window_aggregate_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    expected_mapping = {
        time_series_table_input_node.parameters.id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -7, 'MONTH')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=3),
        }
    }
    check_partition_column_filters(partition_column_filters, expected_mapping)


def test_scd_lookup_feature(global_graph, scd_lookup_feature_node, min_max_point_in_time, adapter):
    """
    Test that partition filters are not generated for SCD lookup features
    """
    _ = scd_lookup_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    assert partition_column_filters == PartitionColumnFilters(mapping={})


def test_latest_feature_with_unbounded_window(
    global_graph,
    latest_value_offset_without_window_feature_node,
    min_max_point_in_time,
    adapter,
):
    """
    Test that partition filters are not generated for latest features with unbounded windows
    """
    _ = latest_value_offset_without_window_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    assert partition_column_filters == PartitionColumnFilters(mapping={})


def test_mixed_features(
    global_graph,
    window_aggregate_on_view_with_scd_join_feature_node,
    time_series_window_aggregate_feature_node,
    time_series_window_aggregate_with_offset_feature_node,
    event_table_input_node_with_id,
    time_series_table_input_node,
    min_max_point_in_time,
    adapter,
):
    """
    Test that partition filters are generated correctly for a mix of features
    """
    _ = window_aggregate_on_view_with_scd_join_feature_node
    _ = time_series_window_aggregate_feature_node
    _ = time_series_window_aggregate_with_offset_feature_node
    partition_column_filters = get_partition_filters_from_graph(
        global_graph,
        *min_max_point_in_time,
        adapter,
    )
    expected_mapping = {
        event_table_input_node_with_id.parameters.id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -129600, 'MINUTE')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=1),
        },
        time_series_table_input_node.parameters.id: {
            "from_timestamp": "DATE_ADD(CAST('2023-01-01 00:00:00' AS TIMESTAMP), -14400, 'MINUTE')",
            "to_timestamp": "CAST('2023-06-01 00:00:00' AS TIMESTAMP)",
            "buffer": TimeInterval(unit="MONTH", value=1),
        },
    }
    check_partition_column_filters(partition_column_filters, expected_mapping)
