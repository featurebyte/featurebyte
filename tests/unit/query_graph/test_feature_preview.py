"""
Tests for feature preview SQL generation
"""

import pandas as pd
import pytest

from featurebyte.enum import InternalName, SourceType
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.cron import (
    JobScheduleTable,
    JobScheduleTableSet,
    get_unique_cron_feature_job_settings,
)
from featurebyte.query_graph.sql.feature_preview import get_feature_or_target_preview_sql
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.util.helper import assert_equal_with_expected_fixture


def test_get_feature_preview_sql(query_graph_with_groupby, source_info, update_fixtures):
    """Test generated preview SQL is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )

    assert_equal_with_expected_fixture(
        preview_sql, "tests/fixtures/expected_preview_sql.sql", update_fixture=update_fixtures
    )


def test_get_feature_preview_sql__category_groupby(
    query_graph_with_category_groupby, source_info, update_fixtures
):
    """Test generated preview SQL with category groupby is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_category_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_category.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__category_groupby_multiple(
    query_graph_with_category_groupby_multiple, source_info, update_fixtures
):
    """Test generated preview SQL with category groupby is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, node = query_graph_with_category_groupby_multiple
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_category_multiple.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__multiple_nodes(
    query_graph_with_similar_groupby_nodes, source_info, update_fixtures
):
    """Test case for preview SQL for list of nodes of similar groupby operations"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    groupby_nodes, graph = query_graph_with_similar_groupby_nodes
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=groupby_nodes,
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_multiple_nodes.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__complex_feature(
    complex_feature_query_graph, source_info, update_fixtures
):
    """Test case for preview SQL for complex feature (combining two different features)"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    node, graph = complex_feature_query_graph
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_complex_feature.sql",
        update_fixture=update_fixtures,
    )


@pytest.mark.parametrize("input_details", ["databricks"], indirect=True)
def test_get_feature_preview_sql__databricks(query_graph_with_groupby, update_fixtures):
    """Test generated preview SQL is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=SourceInfo(
            database_name="my_db",
            schema_name="my_schema",
            source_type=SourceType.DATABRICKS,
        ),
    )

    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_databricks.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__item_groupby(
    mixed_point_in_time_and_item_aggregations,
    source_info,
    update_fixtures,
):
    """Test case for preview SQL for a feature list with both point-in-time and item aggregations"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, groupby_node, item_groupby_feature_node = mixed_point_in_time_and_item_aggregations
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[groupby_node, item_groupby_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_item_groupby.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__double_aggregation(
    global_graph,
    order_size_agg_by_cust_id_graph,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a feature involving double aggregation
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, node = order_size_agg_by_cust_id_graph
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_double_aggregation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__lookup_features(
    global_graph,
    lookup_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a lookup feature
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = global_graph
    node = lookup_feature_node
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__event_lookup_features(
    global_graph,
    event_lookup_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a lookup feature from EventTable
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "ORDER_ID": 1000,
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[event_lookup_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_event_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__scd_lookup_features(
    global_graph,
    scd_lookup_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a SCD lookup feature
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = global_graph
    node = scd_lookup_feature_node
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_scd_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__scd_lookup_features_with_offset(
    global_graph,
    scd_offset_lookup_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a SCD lookup feature with offset
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = global_graph
    node = scd_offset_lookup_feature_node
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_scd_lookup_with_offset.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__snapshots_lookup_features(
    global_graph,
    snapshots_lookup_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a SCD lookup feature
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = global_graph
    node = snapshots_lookup_feature_node
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_snapshots_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__latest_aggregation(
    global_graph,
    latest_value_aggregation_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for a latest aggregation feature with bounded window
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[latest_value_aggregation_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_latest_aggregation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__latest_aggregation_no_window(
    global_graph,
    latest_value_without_window_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for latest aggregation feature without window
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[latest_value_without_window_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_latest_aggregation_no_window.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__aggregate_asat(
    global_graph,
    aggregate_asat_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for aggregate as at feature
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[aggregate_asat_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_aggregate_asat.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__aggregate_asat_offset(
    global_graph,
    aggregate_asat_with_offset_feature_node,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for aggregate as at feature with offset
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[aggregate_asat_with_offset_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_aggregate_asat_offset.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__all_types(
    global_graph,
    feature_nodes_all_types,
    source_info,
    update_fixtures,
):
    """
    Test case for preview SQL for multiple feature types (window aggregation, item aggregation,
    dimension lookup, scd lookup)
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = global_graph
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=feature_nodes_all_types,
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_all_types.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__with_missing_value_imputation(
    query_graph_with_cleaning_ops_and_groupby, source_info, update_fixtures
):
    """Test generated preview SQL is as expected (missing value imputation on column "a")"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, node = query_graph_with_cleaning_ops_and_groupby
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_with_missing_value_imputation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__with_parent_serving_preparation(
    query_graph_with_cleaning_ops_and_groupby,
    parent_serving_preparation,
    source_info,
    update_fixtures,
):
    """Test sql generation with parent serving preparation"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, node = query_graph_with_cleaning_ops_and_groupby

    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
        parent_serving_preparation=parent_serving_preparation,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_with_parent_serving_preparation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__on_demand_features(
    global_graph,
    time_since_last_event_feature_node,
    source_info,
    update_fixtures,
):
    """Test sql generation for on-demand features"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[time_since_last_event_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_on_demand_features.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__window_offset(
    global_graph, window_aggregate_with_offset_feature_node, source_info, update_fixtures
):
    """Test generated preview SQL for window aggregation with offset is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[window_aggregate_with_offset_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_window_offset.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__non_tile_window_aggregate(
    global_graph, non_tile_window_aggregate_feature_node, source_info, update_fixtures
):
    """Test generated preview SQL for window aggregation with offset is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[non_tile_window_aggregate_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_non_tile_window_aggregate.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__time_series_window_aggregate(
    global_graph,
    time_series_window_aggregate_feature_node,
    source_info,
    column_statistics_info,
    update_fixtures,
):
    """Test generated preview SQL for time series window aggregate"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    cron_feature_job_settings = get_unique_cron_feature_job_settings(
        global_graph, [time_series_window_aggregate_feature_node], SourceType.SNOWFLAKE
    )
    job_schedule_table_set = JobScheduleTableSet(
        tables=[
            JobScheduleTable(
                table_name="job_schedule_1",
                cron_feature_job_setting=cron_feature_job_settings[0],
                job_schedule_dataframe=pd.DataFrame([
                    {
                        InternalName.CRON_JOB_SCHEDULE_DATETIME: pd.Timestamp(
                            "2022-04-20 18:00:00"
                        ),
                        InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC: pd.Timestamp(
                            "2022-04-20 10:00:00"
                        ),
                    }
                ]),
            )
        ],
    )
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[time_series_window_aggregate_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
        job_schedule_table_set=job_schedule_table_set,
        column_statistics_info=column_statistics_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_time_series_window_aggregate.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__days_until_forecast_point(
    days_until_forecast_feature,
    source_info,
    update_fixtures,
):
    """Test sql generation for features using FORECAST_POINT request column"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
        "FORECAST_POINT": "2022-05-01",
    }
    preview_sql = get_feature_or_target_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=days_until_forecast_feature.graph,
        nodes=[days_until_forecast_feature.node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_days_until_forecast_point.sql",
        update_fixture=update_fixtures,
    )
