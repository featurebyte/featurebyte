"""
Tests for featurebyte.query_graph.sql.online_serving
"""

from typing import List

import textwrap
from unittest.mock import patch

import pandas as pd
import pytest
from bson import ObjectId
from feast.online_response import TIMESTAMP_POSTFIX

from featurebyte.enum import SourceType
from featurebyte.models.feature_query_set import FeatureQuerySet
from featurebyte.query_graph.enum import FEAST_TIMESTAMP_POSTFIX
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.batch_helper import get_feature_names
from featurebyte.query_graph.sql.common import sql_to_string
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.online_serving import (
    fill_version_placeholders_for_query_set,
    get_aggregation_result_names,
    get_online_features_query_set,
    get_online_store_retrieval_expr,
    is_online_store_eligible,
)
from featurebyte.query_graph.sql.online_store_compute_query import (
    OnlineStorePrecomputePlan,
    get_online_store_precompute_queries,
)
from featurebyte.query_graph.sql.specs import TileBasedAggregationSpec
from tests.util.helper import assert_equal_with_expected_fixture


def feature_query_set_to_string(feature_query_set: FeatureQuerySet) -> str:
    """
    Convert a feature query set to string for testing purpose
    """

    def _to_string(str_or_expr):
        if isinstance(str_or_expr, str):
            query_str = str_or_expr
        else:
            query_str = sql_to_string(str_or_expr, SourceType.SNOWFLAKE)
        return query_str

    all_queries = []
    for feature_query in feature_query_set.feature_queries:
        all_queries.append(_to_string(feature_query.sql))
    all_queries.append(_to_string(feature_query_set.output_query))

    return ";\n\n".join(all_queries)


@pytest.fixture
def adapter():
    """
    Fixture for a sql adapter
    """
    return get_sql_adapter(SourceType.SNOWFLAKE)


@pytest.fixture(name="mocked_unique_identifier_generator", autouse=True)
def mocked_unique_identifier_generator_fixture():
    """
    Patch ObjectId to return a fixed value so that queries are deterministic
    """
    with patch("featurebyte.query_graph.sql.online_serving.ObjectId") as patched_object_id:
        patched_object_id.return_value = ObjectId("000000000000000000000000")
        yield patched_object_id


def get_aggregation_specs(graph, groupby_node) -> List[TileBasedAggregationSpec]:
    agg_specs = TileBasedAggregationSpec.from_groupby_query_node(
        graph, groupby_node, get_sql_adapter(SourceType.SNOWFLAKE), True
    )
    return agg_specs


def test_construct_universe_sql(query_graph_with_groupby):
    """
    Test constructing universe sql for a simple point in time groupby
    """
    node = query_graph_with_groupby.get_node_by_name("groupby_1")
    plan = OnlineStorePrecomputePlan(
        query_graph_with_groupby, node, get_sql_adapter(SourceType.SNOWFLAKE), True
    )
    agg_specs = get_aggregation_specs(query_graph_with_groupby, node)

    # window size of 2h
    universe = plan._construct_online_store_universe(agg_specs[0])
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID"
        FROM TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725
        WHERE
          INDEX >= FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          ) - 2
          AND INDEX < FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          )
        """
    ).strip()
    assert universe.expr.sql(pretty=True) == expected_sql

    # window size of 48h
    universe = plan._construct_online_store_universe(agg_specs[1])
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID"
        FROM TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725
        WHERE
          INDEX >= FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          ) - 48
          AND INDEX < FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          )
        """
    ).strip()
    assert universe.expr.sql(pretty=True) == expected_sql


def test_construct_universe_sql__category(query_graph_with_category_groupby):
    """
    Test constructing universe sql for groupby with category (the category column should not be part
    of SELECT DISTINCT)
    """
    graph = query_graph_with_category_groupby
    node = graph.get_node_by_name("groupby_1")
    plan = OnlineStorePrecomputePlan(graph, node, get_sql_adapter(SourceType.SNOWFLAKE), True)
    agg_specs = get_aggregation_specs(graph, node)
    universe = plan._construct_online_store_universe(agg_specs[0])
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID"
        FROM TILE_F3600_M1800_B900_FEB86FDFF3B041DC98880F9B22EE9078FBCF5226
        WHERE
          INDEX >= FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          ) - 2
          AND INDEX < FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          )
        """
    ).strip()
    assert universe.expr.sql(pretty=True) == expected_sql


def test_construct_universe_sql__unbounded_latest(
    global_graph, latest_value_without_window_feature_node
):
    """
    Test constructing universe sql for groupby with category
    """
    plan = OnlineStorePrecomputePlan(
        global_graph,
        latest_value_without_window_feature_node,
        get_sql_adapter(SourceType.SNOWFLAKE),
        True,
    )
    agg_specs = get_aggregation_specs(global_graph, global_graph.get_node_by_name("groupby_1"))
    universe = plan._construct_online_store_universe(agg_specs[0])
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID",
          "biz_id" AS "BUSINESS_ID"
        FROM TILE_F3600_M1800_B900_AF1FD0AEE34EC80A96A6D5A486CE40F5A2267B4E
        WHERE
          INDEX < FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          )
        """
    ).strip()
    assert universe.expr.sql(pretty=True) == expected_sql


def test_construct_universe_sql__window_offset(
    global_graph, window_aggregate_with_offset_feature_node
):
    """
    Test constructing universe sql for window aggregate with offset
    """
    plan = OnlineStorePrecomputePlan(
        global_graph,
        window_aggregate_with_offset_feature_node,
        get_sql_adapter(SourceType.SNOWFLAKE),
        True,
    )
    agg_specs = get_aggregation_specs(global_graph, global_graph.get_node_by_name("groupby_1"))
    universe = plan._construct_online_store_universe(agg_specs[0])
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID"
        FROM TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725
        WHERE
          INDEX >= FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          ) - 24 - 8
          AND INDEX < FLOOR(
            (
              DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800
            ) / 3600
          ) - 8
        """
    ).strip()
    assert universe.expr.sql(pretty=True) == expected_sql


def test_is_online_store_eligible__non_time_aware(global_graph, order_size_feature_node):
    """
    Test is_online_store_eligible for a non-time-aware feature node
    """
    assert not is_online_store_eligible(global_graph, order_size_feature_node)


def test_is_online_store_eligible__window_feature(
    global_graph, window_aggregate_with_offset_feature_node
):
    """
    Test is_online_store_eligible for latest value without window
    """
    assert is_online_store_eligible(global_graph, window_aggregate_with_offset_feature_node)


def test_is_online_store_eligible__latest_without_window(
    global_graph, latest_value_without_window_feature_node
):
    """
    Test is_online_store_eligible for latest value without window
    """
    assert not is_online_store_eligible(global_graph, latest_value_without_window_feature_node)


def test_online_store_feature_compute_sql(query_graph_with_groupby, update_fixtures):
    """
    Test constructing feature sql for online store
    """
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    queries = get_online_store_precompute_queries(graph, node, SourceType.SNOWFLAKE, True)
    assert len(queries) == 2
    tile_id = "8502F6BC497F17F84385ABE4346FD392F2F56725"
    aggregation_id = "f37862722c21105449ad882409cf62a1ff7f5b35"
    expected_query_params = {
        "tile_id": f"TILE_F3600_M1800_B900_{tile_id}",
        "aggregation_id": f"avg_{aggregation_id}",
        "table_name": "online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc",
        "result_type": "FLOAT",
        "serving_names": ["CUSTOMER_ID"],
    }
    assert (
        queries[0].dict(exclude={"sql"}).items()
        >= {
            "result_name": f"_fb_internal_CUSTOMER_ID_window_w7200_avg_{aggregation_id}",
            **expected_query_params,
        }.items()
    )
    assert (
        queries[1].dict(exclude={"sql"}).items()
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


def test_complex_features(complex_feature_query_graph, adapter, update_fixtures):
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
    queries = get_online_store_precompute_queries(
        pruned_graph, pruned_node, SourceType.SNOWFLAKE, True
    )
    assert len(queries) == 2
    expected_query_params_tile_1 = {
        "tile_id": "TILE_F3600_M1800_B900_8502F6BC497F17F84385ABE4346FD392F2F56725",
        "aggregation_id": "avg_f37862722c21105449ad882409cf62a1ff7f5b35",
        "table_name": "online_store_b3bad6f0a450e950306704a0ef7bd384756a05cc",
        "result_type": "FLOAT",
        "serving_names": ["CUSTOMER_ID"],
    }
    expected_query_params_tile_2 = {
        "tile_id": "TILE_F3600_M1800_B900_7BD30FF1B8E84ADD2B289714C473F1A21E9BC624",
        "aggregation_id": "sum_d5ebb5711120ac12cb84f6136654c6dba7e21774",
        "table_name": "online_store_51064268424bf868a2ea2dc2f5789e7cb4df29bf",
        "result_type": "FLOAT",
        "serving_names": ["BUSINESS_ID"],
    }
    assert (
        queries[0].dict(exclude={"sql"}).items()
        >= {
            "result_name": "_fb_internal_CUSTOMER_ID_window_w7200_avg_f37862722c21105449ad882409cf62a1ff7f5b35",
            **expected_query_params_tile_1,
        }.items()
    )
    assert (
        queries[1].dict(exclude={"sql"}).items()
        >= {
            "result_name": "_fb_internal_BUSINESS_ID_window_w604800_sum_d5ebb5711120ac12cb84f6136654c6dba7e21774",
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
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_complex.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_2h_avg_by_user_div_7d_by_biz"]


def test_online_store_feature_retrieval_sql__all_eligible(
    query_graph_with_groupby_and_feature_nodes, update_fixtures, adapter
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
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_simple.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_2h_average", "a_48h_average plus 123"]


def test_online_store_feature_retrieval_sql__mixed(
    mixed_point_in_time_and_item_aggregations_features, adapter, update_fixtures
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
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_mixed.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_48h_average", "order_size"]


def test_online_store_feature_retrieval_sql__request_subquery(
    mixed_point_in_time_and_item_aggregations_features, adapter, update_fixtures
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
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_request_subquery.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["a_48h_average", "order_size"]


def test_online_store_feature_retrieval_sql__scd_lookup_with_current_flag_column(
    global_graph, scd_lookup_feature_node, adapter, update_fixtures
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
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        expr.sql(pretty=True),
        "tests/fixtures/expected_online_feature_retrieval_scd_current_flag.sql",
        update_fixture=update_fixtures,
    )
    assert feature_names == ["Current Membership Status"]


def test_online_store_feature_retrieval_sql__version_placeholders_filled(
    mixed_point_in_time_and_item_aggregations_features, update_fixtures
):
    """
    Test retrieval sql with version placeholders filled (single
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    feature_query_set = get_online_features_query_set(
        graph=graph,
        node_groups=[nodes],
        output_feature_names=get_feature_names(graph, nodes),
        source_type=SourceType.SNOWFLAKE,
        request_table_columns=["CUSTOMER_ID", "order_id"],
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_name=None,
    )
    aggregation_result_names = get_aggregation_result_names(
        graph=graph,
        nodes=nodes,
        source_type=SourceType.SNOWFLAKE,
    )
    versions_mapping = {k: i for (i, k) in enumerate(sorted(aggregation_result_names))}
    fill_version_placeholders_for_query_set(feature_query_set, versions_mapping)
    sql = sql_to_string(feature_query_set.output_query, SourceType.SNOWFLAKE)
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_filled_versions.sql",
        update_fixture=update_fixtures,
    )


def test_online_store_feature_retrieval_sql__multiple_groups(
    mixed_point_in_time_and_item_aggregations_features, update_fixtures
):
    """
    Test retrieval sql with version placeholders filled
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    feature_query_set = get_online_features_query_set(
        graph=graph,
        node_groups=[nodes[:1], nodes[1:]],
        output_feature_names=get_feature_names(graph, nodes),
        source_type=SourceType.SNOWFLAKE,
        request_table_columns=["CUSTOMER_ID", "order_id"],
        request_table_details=TableDetails(table_name="MY_REQUEST_TABLE"),
        request_table_name="REQUEST_TABLE_1234",
    )
    aggregation_result_names = get_aggregation_result_names(
        graph=graph,
        nodes=nodes,
        source_type=SourceType.SNOWFLAKE,
    )
    versions_mapping = {k: i for (i, k) in enumerate(sorted(aggregation_result_names))}
    fill_version_placeholders_for_query_set(feature_query_set, versions_mapping)
    sql = feature_query_set_to_string(feature_query_set)
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_filled_versions_multiple_groups.sql",
        update_fixture=update_fixtures,
    )


def test_feast_timestamp_postfix_consistency():
    """This test is used to check the consistency of the FEAST_TIMESTAMP_POSTFIX constant"""
    assert FEAST_TIMESTAMP_POSTFIX == TIMESTAMP_POSTFIX == "__ts"
