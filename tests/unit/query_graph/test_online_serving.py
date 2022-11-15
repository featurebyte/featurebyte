"""
Tests for featurebyte.query_graph.sql.online_serving
"""
import textwrap

import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.online_serving import (
    OnlineStoreUniversePlan,
    get_online_store_feature_compute_sql,
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
