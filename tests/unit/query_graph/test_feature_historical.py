"""
Tests for featurebyte.query_graph.feature_historical.py
"""

from datetime import datetime

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from featurebyte.models.tile import OnDemandTileTable
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.ast.literal import make_literal_value
from featurebyte.query_graph.sql.batch_helper import get_feature_names
from featurebyte.query_graph.sql.common import (
    REQUEST_TABLE_NAME,
    PartitionColumnFilter,
    PartitionColumnFilters,
    sql_to_string,
)
from featurebyte.query_graph.sql.feature_historical import (
    PROGRESS_MESSAGE_COMPUTING_FEATURES,
    convert_point_in_time_dtype_if_needed,
    get_historical_features_expr,
    get_historical_features_query_set,
    get_internal_observation_set,
    validate_historical_requests_point_in_time,
)
from tests.util.helper import assert_equal_with_expected_fixture, feature_query_set_to_string


def get_historical_features_sql(**kwargs):
    """Get historical features SQL"""
    query_plan = get_historical_features_expr(**kwargs)
    source_info = kwargs["source_info"]
    return sql_to_string(query_plan.get_standalone_expr(), source_type=source_info.source_type)


@pytest.fixture(name="output_table_details")
def output_table_details_fixture():
    """Fixture for a TableDetails for the output location"""
    return TableDetails(table_name="SOME_HISTORICAL_FEATURE_TABLE")


def test_get_historical_feature_sql(float_feature, source_info, update_fixtures):
    """Test SQL code generated for historical features is expected"""
    request_table_columns = ["POINT_IN_TIME", "cust_id", "A", "B", "C"]
    sql = get_historical_features_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        sql, "tests/fixtures/expected_historical_requests.sql", update_fixture=update_fixtures
    )


def test_get_historical_feature_sql__serving_names_mapping(
    float_feature, source_info, update_fixtures
):
    """Test SQL code generated for historical features with serving names mapping"""
    request_table_columns = ["POINT_IN_TIME", "NEW_CUST_ID", "A", "B", "C"]
    serving_names_mapping = {"cust_id": "NEW_CUST_ID"}
    sql = get_historical_features_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        serving_names_mapping=serving_names_mapping,
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_historical_requests_with_mapping.sql",
        update_fixture=update_fixtures,
    )


def test_validate_historical_requests_point_in_time():
    """Test validate_historical_requests_point_in_time work with timestamps that contain timezone"""
    original_observation_set = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2020-01-01T10:00:00+08:00", freq="D", periods=10),
    })
    observation_set = original_observation_set.copy()

    # this should not fail and convert point-in-time values to UTC
    converted_observation_set = convert_point_in_time_dtype_if_needed(observation_set)
    validate_historical_requests_point_in_time(
        get_internal_observation_set(converted_observation_set)
    )
    expected_df = pd.DataFrame({
        "POINT_IN_TIME": pd.date_range("2020-01-01T02:00:00", freq="D", periods=10),
    })
    assert_frame_equal(converted_observation_set, expected_df)

    # observation_set should not be modified
    assert_frame_equal(observation_set, original_observation_set)


def test_get_historical_feature_sql__with_missing_value_imputation(
    query_graph_with_cleaning_ops_and_groupby, source_info, update_fixtures
):
    """Test SQL code generated for historical features constructed with missing value imputation"""
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    graph, node = query_graph_with_cleaning_ops_and_groupby
    sql = get_historical_features_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        request_table_columns=request_table_columns,
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_historical_requests_with_missing_value_imputation.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__single_batch(
    float_feature, output_table_details, source_info, update_fixtures
):
    """
    Test historical features are calculated in single batch when there are not many nodes
    """
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=[float_feature.name],
        progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
    )
    output_query = feature_query_set_to_string(query_set, [float_feature.node], source_info)
    assert query_set.progress_message == PROGRESS_MESSAGE_COMPUTING_FEATURES
    assert_equal_with_expected_fixture(
        output_query,
        "tests/fixtures/expected_historical_requests_single_batch_output_query.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__multiple_batches(
    global_graph,
    feature_nodes_all_types,
    output_table_details,
    source_info,
    update_fixtures,
):
    """
    Test historical features are executed in batches when there are many nodes
    """
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=feature_nodes_all_types,
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=get_feature_names(global_graph, feature_nodes_all_types),
    )
    output_query = feature_query_set_to_string(
        query_set, feature_nodes_all_types, source_info, num_features_per_query=2
    )
    assert_equal_with_expected_fixture(
        output_query,
        "tests/fixtures/expected_historical_requests_multiple_batches_output_query.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__output_include_row_index(
    float_feature, output_table_details, update_fixtures, source_info
):
    """
    Test historical features table include row index column if specified
    """
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=[float_feature.name],
        output_include_row_index=True,
        progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
    )
    assert query_set.progress_message == PROGRESS_MESSAGE_COMPUTING_FEATURES
    output_query = feature_query_set_to_string(query_set, [float_feature.node], source_info)
    assert_equal_with_expected_fixture(
        output_query,
        "tests/fixtures/expected_historical_requests_output_row_index.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__on_demand_tile_tables(
    float_feature, output_table_details, update_fixtures, source_info
):
    """
    Test historical features table when on demand tile tables are provided
    """
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    graph = float_feature.graph
    groupby_node_name = graph.get_input_node_names(float_feature.node)[0]
    groupby_node = graph.get_node_by_name(groupby_node_name)
    on_demand_tile_tables = [
        OnDemandTileTable(
            tile_table_id=groupby_node.parameters.tile_id,
            on_demand_table_name="__MY_TEMP_TILE_TABLE",
        )
    ]
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=float_feature.graph,
        nodes=[float_feature.node],
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=[float_feature.name],
        output_include_row_index=True,
        on_demand_tile_tables=on_demand_tile_tables,
        progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
    )
    assert query_set.progress_message == PROGRESS_MESSAGE_COMPUTING_FEATURES
    output_query = feature_query_set_to_string(query_set, [float_feature.node], source_info)
    assert "__MY_TEMP_TILE_TABLE" in output_query
    assert_equal_with_expected_fixture(
        output_query,
        "tests/fixtures/expected_historical_requests_on_demand_tile_tables.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__partition_column_filters(
    ts_window_aggregate_feature,
    snowflake_time_series_table,
    output_table_details,
    update_fixtures,
    source_info,
):
    """
    Test historical features table when partition column filters are provided
    """
    feature = ts_window_aggregate_feature
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    partition_column_filters = PartitionColumnFilters(
        mapping={
            snowflake_time_series_table.id: PartitionColumnFilter(
                from_timestamp=make_literal_value(
                    datetime(2023, 1, 1, 0, 0, 0), cast_as_timestamp=True
                ),
                to_timestamp=make_literal_value(
                    datetime(2023, 6, 1, 0, 0, 0), cast_as_timestamp=True
                ),
            )
        }
    )
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=[feature.name],
        output_include_row_index=True,
        partition_column_filters=partition_column_filters,
        progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
    )
    assert query_set.progress_message == PROGRESS_MESSAGE_COMPUTING_FEATURES
    output_query = feature_query_set_to_string(query_set, [feature.node], source_info)
    assert (
        "\"date\" >= TO_CHAR(CAST('2023-01-01 00:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')"
        in output_query
    )
    assert (
        "\"date\" <= TO_CHAR(CAST('2023-06-01 00:00:00' AS TIMESTAMP), 'YYYY-MM-DD HH24:MI:SS')"
        in output_query
    )
    assert_equal_with_expected_fixture(
        output_query,
        "tests/fixtures/expected_historical_requests_partition_column_filters.sql",
        update_fixture=update_fixtures,
    )


def test_get_historical_feature_query_set__development_dataset(
    ts_window_aggregate_feature,
    snowflake_time_series_table,
    snowflake_time_series_table_development_dataset,
    output_table_details,
    update_fixtures,
    source_info,
):
    """
    Test historical features table when development datasets are provided
    """
    feature = ts_window_aggregate_feature
    request_table_columns = ["POINT_IN_TIME", "CUSTOMER_ID"]
    development_datasets = snowflake_time_series_table_development_dataset.to_development_datasets()
    query_set = get_historical_features_query_set(
        request_table_name=REQUEST_TABLE_NAME,
        graph=feature.graph,
        nodes=[feature.node],
        request_table_columns=request_table_columns,
        source_info=source_info,
        output_table_details=output_table_details,
        output_feature_names=[feature.name],
        output_include_row_index=True,
        development_datasets=development_datasets,
        progress_message=PROGRESS_MESSAGE_COMPUTING_FEATURES,
    )
    assert query_set.progress_message == PROGRESS_MESSAGE_COMPUTING_FEATURES
    output_query = feature_query_set_to_string(query_set, [feature.node], source_info)
    assert '"db"."schema"."sf_time_series_table_dev_sampled"' in output_query
    assert '"db"."schema"."sf_time_series_table"' not in output_query
    assert_equal_with_expected_fixture(
        output_query,
        "tests/fixtures/expected_historical_requests_development_dataset.sql",
        update_fixture=update_fixtures,
    )
