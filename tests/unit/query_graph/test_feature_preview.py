"""
Tests for feature preview SQL generation
"""
from featurebyte.query_graph.feature_preview import get_feature_preview_sql
from tests.helper.helper import assert_equal_with_expected_fixture


def test_get_feature_preview_sql(query_graph_with_groupby):
    """Test generated preview SQL is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_preview_sql(
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name=point_in_time_and_serving_name,
    )

    assert_equal_with_expected_fixture(
        preview_sql, "tests/fixtures/expected_preview_sql.sql", update_fixture=False
    )


def test_get_feature_preview_sql__category_groupby(query_graph_with_category_groupby):
    """Test generated preview SQL with category groupby is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_category_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_feature_preview_sql(
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name=point_in_time_and_serving_name,
    )
    assert_equal_with_expected_fixture(
        preview_sql, "tests/fixtures/expected_preview_sql_category.sql", update_fixture=False
    )
