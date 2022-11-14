"""
Tests for featurebyte.query_graph.sql.online_serving
"""
import textwrap

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.online_serving import OnlineStoreUniversePlan


def test_construct_universe_sql(query_graph_with_groupby):
    """
    Test constructing universe sql for a simple point in time groupby
    """
    plan = OnlineStoreUniversePlan(query_graph_with_groupby, get_sql_adapter(SourceType.SNOWFLAKE))
    node = query_graph_with_groupby.get_node_by_name("groupby_1")
    plan.update(node)
    expr = plan.construct_online_store_universe_expr()
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          SYSDATE() AS POINT_IN_TIME,
          "cust_id"
        FROM fake_transactions_table_f3600_m1800_b900_fa69ec6e12d9162469e8796a5d93c8a1e767dc0d
        WHERE
          INDEX >= FLOOR((DATE_PART(EPOCH_SECOND, SYSDATE()) - 1800) / 3600) - 48
          AND INDEX < FLOOR((DATE_PART(EPOCH_SECOND, SYSDATE()) - 1800) / 3600)
        """
    ).strip()
    assert expr.sql(pretty=True) == expected_sql


def test_construct_universe_sql__category(query_graph_with_category_groupby):
    """
    Test constructing universe sql for groupby with category
    """
    graph = query_graph_with_category_groupby
    plan = OnlineStoreUniversePlan(graph, get_sql_adapter(SourceType.SNOWFLAKE))
    node = graph.get_node_by_name("groupby_1")
    plan.update(node)
    expr = plan.construct_online_store_universe_expr()
    expected_sql = textwrap.dedent(
        """
        SELECT DISTINCT
          SYSDATE() AS POINT_IN_TIME,
          "cust_id"
        FROM fake_transactions_table_f3600_m1800_b900_422275c11ff21e200f4c47e66149f25c404b7178
        WHERE
          INDEX >= FLOOR((DATE_PART(EPOCH_SECOND, SYSDATE()) - 1800) / 3600) - 48
          AND INDEX < FLOOR((DATE_PART(EPOCH_SECOND, SYSDATE()) - 1800) / 3600)
        """
    ).strip()
    assert expr.sql(pretty=True) == expected_sql
