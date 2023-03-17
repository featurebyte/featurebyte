"""
Tests for feature preview SQL generation
"""
import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.feature_preview import get_feature_preview_sql
from tests.util.helper import assert_equal_with_expected_fixture


def test_get_feature_preview_sql(query_graph_with_groupby, update_fixtures):
    """Test generated preview SQL is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )

    assert_equal_with_expected_fixture(
        preview_sql, "tests/fixtures/expected_preview_sql.sql", update_fixture=update_fixtures
    )


def test_get_feature_preview_sql__category_groupby(
    query_graph_with_category_groupby, update_fixtures
):
    """Test generated preview SQL with category groupby is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_category_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_category.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__multiple_nodes(
    query_graph_with_similar_groupby_nodes, update_fixtures
):
    """Test case for preview SQL for list of nodes of similar groupby operations"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    groupby_nodes, graph = query_graph_with_similar_groupby_nodes
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=groupby_nodes,
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_multiple_nodes.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__complex_feature(complex_feature_query_graph, update_fixtures):
    """Test case for preview SQL for complex feature (combining two different features)"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    node, graph = complex_feature_query_graph
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
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
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.DATABRICKS,
    )

    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_databricks.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__item_groupby(
    mixed_point_in_time_and_item_aggregations,
    update_fixtures,
):
    """Test case for preview SQL for a feature list with both point-in-time and item aggregations"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, groupby_node, item_groupby_feature_node = mixed_point_in_time_and_item_aggregations
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[groupby_node, item_groupby_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_item_groupby.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__double_aggregation(
    global_graph,
    order_size_agg_by_cust_id_graph,
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
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_double_aggregation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__lookup_features(
    global_graph,
    lookup_feature_node,
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
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__event_lookup_features(
    global_graph,
    event_lookup_node,
    update_fixtures,
):
    """
    Test case for preview SQL for a lookup feature from EventTable
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "ORDER_ID": 1000,
    }
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[event_lookup_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_event_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__scd_lookup_features(
    global_graph,
    scd_lookup_feature_node,
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
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_scd_lookup.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__scd_lookup_features_with_offset(
    global_graph,
    scd_offset_lookup_feature_node,
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
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_scd_lookup_with_offset.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__latest_aggregation(
    global_graph,
    latest_value_aggregation_feature_node,
    update_fixtures,
):
    """
    Test case for preview SQL for a latest aggregation feature with bounded window
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[latest_value_aggregation_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_latest_aggregation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__latest_aggregation_no_window(
    global_graph,
    latest_value_without_window_feature_node,
    update_fixtures,
):
    """
    Test case for preview SQL for latest aggregation feature without window
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[latest_value_without_window_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_latest_aggregation_no_window.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__aggregate_asat(
    global_graph,
    aggregate_asat_feature_node,
    update_fixtures,
):
    """
    Test case for preview SQL for aggregate as at feature
    """
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=global_graph,
        nodes=[aggregate_asat_feature_node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_aggregate_asat.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__all_types(
    global_graph,
    mixed_point_in_time_and_item_aggregations,
    lookup_feature_node,
    scd_lookup_feature_node,
    latest_value_aggregation_feature_node,
    latest_value_without_window_feature_node,
    aggregate_asat_feature_node,
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
    _, groupby_node, item_groupby_feature_node = mixed_point_in_time_and_item_aggregations
    nodes = [
        groupby_node,
        item_groupby_feature_node,
        lookup_feature_node,
        scd_lookup_feature_node,
        latest_value_aggregation_feature_node,
        latest_value_without_window_feature_node,
        aggregate_asat_feature_node,
    ]
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=nodes,
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_all_types.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__with_missing_value_imputation(
    query_graph_with_cleaning_ops_and_groupby, update_fixtures
):
    """Test generated preview SQL is as expected (missing value imputation on column "a")"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, node = query_graph_with_cleaning_ops_and_groupby
    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_with_missing_value_imputation.sql",
        update_fixture=update_fixtures,
    )


def test_get_feature_preview_sql__with_parent_serving_preparation(
    query_graph_with_cleaning_ops_and_groupby,
    parent_serving_preparation,
    update_fixtures,
):
    """Test sql generation with parent serving preparation"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph, node = query_graph_with_cleaning_ops_and_groupby

    preview_sql = get_feature_preview_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
        parent_serving_preparation=parent_serving_preparation,
    )
    assert_equal_with_expected_fixture(
        preview_sql,
        "tests/fixtures/expected_preview_sql_with_parent_serving_prepration.sql",
        update_fixture=update_fixtures,
    )
