"""
Tests for featurebyte.query_graph.sql.online_serving
"""

from datetime import datetime
from typing import List

import pandas as pd
from feast.online_response import TIMESTAMP_POSTFIX

from featurebyte.query_graph.enum import FEAST_TIMESTAMP_POSTFIX
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.batch_helper import get_feature_names
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.online_serving import (
    get_aggregation_result_names,
    get_online_features_query_set,
    get_online_store_retrieval_expr,
)
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from tests.util.helper import assert_equal_with_expected_fixture, feature_query_set_to_string


def get_aggregation_specs(graph, groupby_node, adapter) -> List[TileBasedAggregationSpec]:
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(graph, groupby_node, adapter, True)
    return agg_specs


def test_complex_features(complex_feature_query_graph, adapter, update_fixtures, source_info):
    """
    Test complex features with multiple tile tables
    """
    node, graph = complex_feature_query_graph

    # Prune graph to remove unused windows (in the actual code paths, graph is always pruned before
    # any sql generation)
    pruned_graph_model, node_name_map = graph.prune(node)
    pruned_node = pruned_graph_model.get_node_by_name(node_name_map[node.name])
    pruned_graph, loaded_node_name_map = QueryGraph().load(pruned_graph_model)
    pruned_node = pruned_graph.get_node_by_name(loaded_node_name_map[pruned_node.name])

    # Check retrieval sql
    query_plan = get_online_store_retrieval_expr(
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_columns=["CUSTOMER_ID"],
        graph=pruned_graph,
        nodes=[pruned_node],
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        query_plan.get_standalone_expr().sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_complex.sql",
        update_fixture=update_fixtures,
    )
    assert query_plan.feature_names == ["a_2h_avg_by_user_div_7d_by_biz"]


def test_online_store_feature_retrieval_sql__all_eligible(
    query_graph_with_groupby_and_feature_nodes, update_fixtures, adapter, source_info
):
    """
    Test constructing feature retrieval sql for online store
    """
    graph, *nodes = query_graph_with_groupby_and_feature_nodes
    query_plan = get_online_store_retrieval_expr(
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_columns=["CUSTOMER_ID"],
        graph=graph,
        nodes=nodes,
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        query_plan.get_standalone_expr().sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_simple.sql",
        update_fixture=update_fixtures,
    )
    assert query_plan.feature_names == ["a_2h_average", "a_48h_average plus 123"]


def test_online_store_feature_retrieval_sql__mixed(
    mixed_point_in_time_and_item_aggregations_features, adapter, update_fixtures, source_info
):
    """
    Test constructing feature retrieval sql for online store where some features cannot be looked up
    from the online store and has to be computed on demand
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    query_plan = get_online_store_retrieval_expr(
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_columns=["CUSTOMER_ID", "order_id"],
        graph=graph,
        nodes=nodes,
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        query_plan.get_standalone_expr().sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_mixed.sql",
        update_fixture=update_fixtures,
    )
    assert query_plan.feature_names == ["a_48h_average", "order_size"]


def test_online_store_feature_retrieval_sql__request_subquery(
    mixed_point_in_time_and_item_aggregations_features, adapter, update_fixtures, source_info
):
    """
    Test constructing feature retrieval sql for online store when request table is a subquery
    """
    df = pd.DataFrame({"CUSTOMER_ID": [1001, 1002, 1003]})
    request_table_expr = construct_dataframe_sql_expr(df, date_cols=[])

    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    query_plan = get_online_store_retrieval_expr(
        request_table_expr=request_table_expr,
        request_table_columns=["CUSTOMER_ID"],
        graph=graph,
        nodes=nodes,
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        query_plan.get_standalone_expr().sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_request_subquery.sql",
        update_fixture=update_fixtures,
    )
    assert query_plan.feature_names == ["a_48h_average", "order_size"]


def test_online_store_feature_retrieval_sql__scd_lookup_with_current_flag_column(
    global_graph, scd_lookup_feature_node, adapter, update_fixtures, source_info
):
    """
    Test constructing feature retrieval sql for online store when request table is a subquery
    """
    df = pd.DataFrame({"CUSTOMER_ID": [1001, 1002, 1003]})
    request_table_expr = construct_dataframe_sql_expr(df, date_cols=[])

    query_plan = get_online_store_retrieval_expr(
        request_table_expr=request_table_expr,
        request_table_columns=["CUSTOMER_ID"],
        graph=global_graph,
        nodes=[scd_lookup_feature_node],
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        query_plan.get_standalone_expr().sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_scd_current_flag.sql",
        update_fixture=update_fixtures,
    )
    assert query_plan.feature_names == ["Current Membership Status"]


def test_online_store_feature_retrieval_sql__version_placeholders_filled(
    mixed_point_in_time_and_item_aggregations_features, update_fixtures, source_info
):
    """
    Test retrieval sql with version placeholders filled (single
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    aggregation_result_names = get_aggregation_result_names(
        graph=graph,
        nodes=nodes,
        source_info=source_info,
    )
    versions_mapping = {k: i for (i, k) in enumerate(sorted(aggregation_result_names))}
    feature_query_set = get_online_features_query_set(
        graph=graph,
        nodes=nodes,
        output_feature_names=get_feature_names(graph, nodes),
        source_info=source_info,
        request_table_columns=["CUSTOMER_ID", "order_id"],
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_name="MY_REQUEST_TABLE",
        request_timestamp=datetime(2023, 10, 1, 12, 0, 0),
        versions=versions_mapping,
    )
    sql = feature_query_set_to_string(feature_query_set, nodes, source_info)
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_filled_versions.sql",
        update_fixture=update_fixtures,
    )


def test_online_store_feature_retrieval_sql__multiple_groups(
    mixed_point_in_time_and_item_aggregations_features, update_fixtures, source_info
):
    """
    Test retrieval sql with version placeholders filled
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    aggregation_result_names = get_aggregation_result_names(
        graph=graph,
        nodes=nodes,
        source_info=source_info,
    )
    versions_mapping = {k: i for (i, k) in enumerate(sorted(aggregation_result_names))}
    feature_query_set = get_online_features_query_set(
        graph=graph,
        nodes=nodes,
        output_feature_names=get_feature_names(graph, nodes),
        source_info=source_info,
        request_table_columns=["CUSTOMER_ID", "order_id"],
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_name="REQUEST_TABLE_1234",
        request_timestamp=datetime(2023, 10, 1, 12, 0, 0),
        versions=versions_mapping,
    )
    sql = feature_query_set_to_string(
        feature_query_set,
        nodes,
        source_info,
        nodes_group_override=[nodes[:1], nodes[1:]],
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_filled_versions_multiple_groups.sql",
        update_fixture=update_fixtures,
    )


def test_feast_timestamp_postfix_consistency():
    """This test is used to check the consistency of the FEAST_TIMESTAMP_POSTFIX constant"""
    assert FEAST_TIMESTAMP_POSTFIX == TIMESTAMP_POSTFIX == "__ts"
