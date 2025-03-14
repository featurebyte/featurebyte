"""
Tests for featurebyte.query_graph.sql.online_serving
"""

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
from featurebyte.query_graph.sql.online_store_compute_query import (
    OnlineStorePrecomputePlan,
    get_online_store_precompute_queries,
)
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from tests.util.helper import assert_equal_with_expected_fixture, feature_query_set_to_string


def get_aggregation_specs(graph, groupby_node, adapter) -> List[TileBasedAggregationSpec]:
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(graph, groupby_node, adapter, True)
    return agg_specs


def test_construct_universe_sql(query_graph_with_groupby, adapter, update_fixtures):
    """
    Test constructing universe sql for a simple point in time groupby
    """
    node = query_graph_with_groupby.get_node_by_name("groupby_1")
    plan = OnlineStorePrecomputePlan(query_graph_with_groupby, node, adapter, True)
    agg_specs = get_aggregation_specs(query_graph_with_groupby, node, adapter)

    # window size of 2h
    universe = plan._construct_online_store_universe(agg_specs[0])
    assert_equal_with_expected_fixture(
        universe.expr.sql(pretty=True),
        "tests/fixtures/expected_universe_2h.sql",
        update_fixture=update_fixtures,
    )

    # window size of 48h
    universe = plan._construct_online_store_universe(agg_specs[1])
    assert_equal_with_expected_fixture(
        universe.expr.sql(pretty=True),
        "tests/fixtures/expected_universe_48h.sql",
        update_fixture=update_fixtures,
    )


def test_construct_universe_sql__category(
    query_graph_with_category_groupby, adapter, source_info, update_fixtures
):
    """
    Test constructing universe sql for groupby with category (the category column should not be part
    of SELECT DISTINCT)
    """
    graph = query_graph_with_category_groupby
    node = graph.get_node_by_name("groupby_1")
    plan = OnlineStorePrecomputePlan(graph, node, adapter, True)
    agg_specs = get_aggregation_specs(graph, node, adapter)
    universe = plan._construct_online_store_universe(agg_specs[0])
    assert_equal_with_expected_fixture(
        universe.expr.sql(pretty=True),
        "tests/fixtures/expected_universe_category.sql",
        update_fixture=update_fixtures,
    )


def test_construct_universe_sql__unbounded_latest(
    global_graph,
    latest_value_without_window_feature_node,
    adapter,
    update_fixtures,
):
    """
    Test constructing universe sql for groupby with category
    """
    plan = OnlineStorePrecomputePlan(
        global_graph,
        latest_value_without_window_feature_node,
        adapter,
        True,
    )
    agg_specs = get_aggregation_specs(
        global_graph, global_graph.get_node_by_name("groupby_1"), adapter
    )
    universe = plan._construct_online_store_universe(agg_specs[0])
    assert_equal_with_expected_fixture(
        universe.expr.sql(pretty=True),
        "tests/fixtures/expected_universe_unbounded_latest.sql",
        update_fixture=update_fixtures,
    )


def test_construct_universe_sql__window_offset(
    global_graph,
    window_aggregate_with_offset_feature_node,
    adapter,
    update_fixtures,
):
    """
    Test constructing universe sql for window aggregate with offset
    """
    plan = OnlineStorePrecomputePlan(
        global_graph,
        window_aggregate_with_offset_feature_node,
        adapter,
        True,
    )
    agg_specs = get_aggregation_specs(
        global_graph, global_graph.get_node_by_name("groupby_1"), adapter
    )
    universe = plan._construct_online_store_universe(agg_specs[0])
    assert_equal_with_expected_fixture(
        universe.expr.sql(pretty=True),
        "tests/fixtures/expected_universe_window_offset.sql",
        update_fixture=update_fixtures,
    )


def test_online_store_feature_compute_sql(query_graph_with_groupby, update_fixtures, adapter):
    """
    Test constructing feature sql for online store
    """
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    queries = get_online_store_precompute_queries(graph, node, adapter, True)
    assert len(queries) == 2
    tile_id = "8502F6BC497F17F84385ABE4346FD392F2F56725"
    aggregation_id = "13c45b8622761dd28afb4640ac3ed355d57d789f"
    expected_query_params = {
        "tile_id": f"TILE_F3600_M1800_B900_{tile_id}",
        "aggregation_id": f"avg_{aggregation_id}",
        "table_name": "online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc".upper(),
        "result_type": "FLOAT",
        "serving_names": ["CUSTOMER_ID"],
    }
    assert (
        queries[0].model_dump(exclude={"sql"}).items()
        >= {
            "result_name": f"_fb_internal_CUSTOMER_ID_window_w7200_avg_{aggregation_id}",
            **expected_query_params,
        }.items()
    )
    assert (
        queries[1].model_dump(exclude={"sql"}).items()
        >= {
            "result_name": f"_fb_internal_CUSTOMER_ID_window_w172800_avg_{aggregation_id}",
            **expected_query_params,
        }.items()
    )
    assert_equal_with_expected_fixture(
        queries[0].sql,
        "tests/fixtures/expected_online_precompute_0.sql",
        update_fixture=update_fixtures,
    )
    assert_equal_with_expected_fixture(
        queries[1].sql,
        "tests/fixtures/expected_online_precompute_1.sql",
        update_fixture=update_fixtures,
    )


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

    # Check precompute sqls
    queries = get_online_store_precompute_queries(pruned_graph, pruned_node, source_info, True)
    assert len(queries) == 2
    expected_query_params_tile_1 = {
        "tile_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "aggregation_id": "avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
        "table_name": "online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc".upper(),
        "result_type": "FLOAT",
        "serving_names": ["CUSTOMER_ID"],
    }
    expected_query_params_tile_2 = {
        "tile_id": "TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624",
        "aggregation_id": "sum_8c11e770ad5121aec588693662ac607b4fba0528",
        "table_name": "online_store_51064268424bf868a2ea2dc2f5789e7cb4df29bf".upper(),
        "result_type": "FLOAT",
        "serving_names": ["BUSINESS_ID"],
    }
    assert (
        queries[0].model_dump(exclude={"sql"}).items()
        >= {
            "result_name": "_fb_internal_CUSTOMER_ID_window_w7200_avg_13c45b8622761dd28afb4640ac3ed355d57d789f",
            **expected_query_params_tile_1,
        }.items()
    )
    assert (
        queries[1].model_dump(exclude={"sql"}).items()
        >= {
            "result_name": "_fb_internal_BUSINESS_ID_window_w604800_sum_8c11e770ad5121aec588693662ac607b4fba0528",
            **expected_query_params_tile_2,
        }.items()
    )
    assert_equal_with_expected_fixture(
        queries[0].sql,
        "tests/fixtures/expected_online_precompute_complex_0.sql",
        update_fixture=update_fixtures,
    )
    assert_equal_with_expected_fixture(
        queries[1].sql,
        "tests/fixtures/expected_online_precompute_complex_1.sql",
        update_fixture=update_fixtures,
    )

    # Check retrieval sql
    expr, feature_names = get_online_store_retrieval_expr(
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_columns=["CUSTOMER_ID"],
        graph=pruned_graph,
        nodes=[pruned_node],
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_complex.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_2h_avg_by_user_div_7d_by_biz"]


def test_online_store_feature_retrieval_sql__all_eligible(
    query_graph_with_groupby_and_feature_nodes, update_fixtures, adapter, source_info
):
    """
    Test constructing feature retrieval sql for online store
    """
    graph, *nodes = query_graph_with_groupby_and_feature_nodes
    expr, feature_names = get_online_store_retrieval_expr(
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_columns=["CUSTOMER_ID"],
        graph=graph,
        nodes=nodes,
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_simple.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_2h_average", "a_48h_average plus 123"]


def test_online_store_feature_retrieval_sql__mixed(
    mixed_point_in_time_and_item_aggregations_features, adapter, update_fixtures, source_info
):
    """
    Test constructing feature retrieval sql for online store where some features cannot be looked up
    from the online store and has to be computed on demand
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    expr, feature_names = get_online_store_retrieval_expr(
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_columns=["CUSTOMER_ID", "order_id"],
        graph=graph,
        nodes=nodes,
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_mixed.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_48h_average", "order_size"]


def test_online_store_feature_retrieval_sql__request_subquery(
    mixed_point_in_time_and_item_aggregations_features, adapter, update_fixtures, source_info
):
    """
    Test constructing feature retrieval sql for online store when request table is a subquery
    """
    df = pd.DataFrame({"CUSTOMER_ID": [1001, 1002, 1003]})
    request_table_expr = construct_dataframe_sql_expr(df, date_cols=[])

    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    expr, feature_names = get_online_store_retrieval_expr(
        request_table_expr=request_table_expr,
        request_table_columns=["CUSTOMER_ID"],
        graph=graph,
        nodes=nodes,
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_request_subquery.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_48h_average", "order_size"]


def test_online_store_feature_retrieval_sql__scd_lookup_with_current_flag_column(
    global_graph, scd_lookup_feature_node, adapter, update_fixtures, source_info
):
    """
    Test constructing feature retrieval sql for online store when request table is a subquery
    """
    df = pd.DataFrame({"CUSTOMER_ID": [1001, 1002, 1003]})
    request_table_expr = construct_dataframe_sql_expr(df, date_cols=[])

    expr, feature_names = get_online_store_retrieval_expr(
        request_table_expr=request_table_expr,
        request_table_columns=["CUSTOMER_ID"],
        graph=global_graph,
        nodes=[scd_lookup_feature_node],
        current_timestamp_expr=adapter.current_timestamp(),
        source_info=source_info,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_scd_current_flag.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["Current Membership Status"]


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
