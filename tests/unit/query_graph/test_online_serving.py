"""
Tests for featurebyte.query_graph.sql.online_serving
"""
import textwrap
from dataclasses import asdict

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.dataframe import construct_dataframe_sql_expr
from featurebyte.query_graph.sql.online_serving import (
    OnlineStoreLookupSpec,
    OnlineStoreUniversePlan,
    get_entities_ids_and_serving_names,
    get_online_store_feature_compute_sql,
    get_online_store_retrieval_sql,
)
from tests.util.helper import assert_equal_with_expected_fixture


def test_construct_universe_sql(query_graph_with_groupby):
    """
    Test constructing universe sql for a simple point in time groupby
    """
    node = query_graph_with_groupby.get_node_by_name("groupby_1")
    plan = OnlineStoreUniversePlan(
        query_graph_with_groupby, node, get_sql_adapter(SourceType.SNOWFLAKE)
    )
    expr, _ = plan.construct_online_store_universe()
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID"
        FROM fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d
        WHERE
          INDEX >= FLOOR((DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800) / 3600) - 48
          AND INDEX < FLOOR((DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800) / 3600)
        """
    ).strip()
    assert expr.sql(pretty=True) == expected_sql


def test_construct_universe_sql__category(query_graph_with_category_groupby):
    """
    Test constructing universe sql for groupby with category
    """
    graph = query_graph_with_category_groupby
    node = graph.get_node_by_name("groupby_1")
    plan = OnlineStoreUniversePlan(graph, node, get_sql_adapter(SourceType.SNOWFLAKE))
    expr, _ = plan.construct_online_store_universe()
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP) AS POINT_IN_TIME,
          "cust_id" AS "CUSTOMER_ID"
        FROM fake_transactions_table_f3600_m1800_b900_422275c11ff21e200f4c47e66149f25c404b7178
        WHERE
          INDEX >= FLOOR((DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800) / 3600) - 48
          AND INDEX < FLOOR((DATE_PART(EPOCH_SECOND, CAST(__FB_POINT_IN_TIME_SQL_PLACEHOLDER AS TIMESTAMP)) - 1800) / 3600)
        """
    ).strip()
    assert expr.sql(pretty=True) == expected_sql


def test_online_store_feature_compute_sql(query_graph_with_groupby, update_fixtures):
    """
    Test constructing feature sql for online store
    """
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    sql = get_online_store_feature_compute_sql(graph, node, SourceType.SNOWFLAKE)
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_compute_sql.sql",
        update_fixture=update_fixtures,
    )


def test_complex_features_not_implemented(complex_feature_query_graph):
    """
    Test complex features with multiple tile tables raises NotImplementedError
    """
    node, graph = complex_feature_query_graph
    with pytest.raises(NotImplementedError):
        _ = get_online_store_feature_compute_sql(graph, node, SourceType.SNOWFLAKE)


def test_get_entities_ids_and_serving_names(mixed_point_in_time_and_item_aggregations_features):
    """
    Test get_entities_ids_and_serving_names
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    # a_48h_average - point in time groupby
    assert get_entities_ids_and_serving_names(graph, nodes[0]) == (
        {ObjectId("637516ebc9c18f5a277a78db")},
        {"CUSTOMER_ID"},
    )
    # order_size - item groupby
    assert get_entities_ids_and_serving_names(graph, nodes[1]) == (set(), set())


def test_online_store_lookup_spec(query_graph_with_groupby_and_feature_nodes):
    """
    Test constructing OnlineStoreLookupSpec from query graph and nodes
    """
    graph, *nodes = query_graph_with_groupby_and_feature_nodes
    assert asdict(OnlineStoreLookupSpec.from_graph_and_node(graph, nodes[0])) == {
        "feature_name": "a_2h_average",
        "feature_store_table_name": "online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6",
        "serving_names": ["CUSTOMER_ID"],
    }
    assert asdict(OnlineStoreLookupSpec.from_graph_and_node(graph, nodes[1])) == {
        "feature_name": "a_48h_average plus 123",
        "feature_store_table_name": "online_store_e5af66c4b0ef5ccf86de19f3403926d5100d9de6",
        "serving_names": ["CUSTOMER_ID"],
    }


def test_online_store_feature_retrieval_sql__all_eligible(
    query_graph_with_groupby_and_feature_nodes, update_fixtures
):
    """
    Test constructing feature retrieval sql for online store
    """
    graph, *nodes = query_graph_with_groupby_and_feature_nodes
    sql = get_online_store_retrieval_sql(
        request_table_name="MY_REQUEST_TABLE",
        request_table_columns=["CUSTOMER_ID"],
        graph=graph,
        nodes=nodes,
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_simple.sql",
        update_fixture=update_fixtures,
    )


def test_online_store_feature_retrieval_sql__mixed(
    mixed_point_in_time_and_item_aggregations_features, update_fixtures
):
    """
    Test constructing feature retrieval sql for online store where some features cannot be looked up
    from the online store and has to be computed on demand
    """
    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    sql = get_online_store_retrieval_sql(
        request_table_name="MY_REQUEST_TABLE",
        request_table_columns=["CUSTOMER_ID", "order_id"],
        graph=graph,
        nodes=nodes,
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_mixed.sql",
        update_fixture=update_fixtures,
    )


def test_online_store_feature_retrieval_sql__request_subquery(
    mixed_point_in_time_and_item_aggregations_features, update_fixtures
):
    """
    Test constructing feature retrieval sql for online store when request table is a subquery
    """
    df = pd.DataFrame({"CUSTOMER_ID": [1001, 1002, 1003]})
    request_table_expr = construct_dataframe_sql_expr(df, date_cols=[])

    graph, *nodes = mixed_point_in_time_and_item_aggregations_features
    sql = get_online_store_retrieval_sql(
        request_table_expr=request_table_expr,
        request_table_columns=["CUSTOMER_ID"],
        graph=graph,
        nodes=nodes,
        source_type=SourceType.SNOWFLAKE,
    )
    assert_equal_with_expected_fixture(
        sql,
        "tests/fixtures/expected_online_feature_retrieval_request_subquery.sql",
        update_fixture=update_fixtures,
    )
