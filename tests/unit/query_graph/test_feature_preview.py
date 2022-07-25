"""
Tests for feature preview SQL generation
"""
from featurebyte.query_graph.feature_preview import get_feature_preview_sql


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

    update_fixture = False
    if update_fixture:
        with open("tests/fixtures/expected_preview_sql.sql", "w", encoding="utf-8") as f_handle:
            f_handle.write(preview_sql)
            f_handle.write("\n")  # make pre-commit hook end-of-file-fixer happy
        raise AssertionError("Fixture updated, please set update_fixture to False")

    with open("tests/fixtures/expected_preview_sql.sql", encoding="utf-8") as f_handle:
        expected = f_handle.read()
    assert preview_sql.strip() == expected.strip()


def test_get_feature_preview_sql__category_groupby(query_graph_with_category_groupby):
    """Test generated preview SQL is as expected"""
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
    raise
