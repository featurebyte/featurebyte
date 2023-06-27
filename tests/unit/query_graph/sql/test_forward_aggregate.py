"""
Test target forward aggregate module
"""
from featurebyte import SourceType
from featurebyte.query_graph.sql.common import REQUEST_TABLE_NAME
from featurebyte.query_graph.sql.forward_aggregate import get_forward_aggregate_sql
from tests.util.helper import assert_equal_with_expected_fixture


def test_get_forward_aggregate_sql(query_graph_with_groupby, update_fixtures):
    """Test generated target forward aggregate SQL is as expected"""
    point_in_time_and_serving_name = {
        "POINT_IN_TIME": "2022-04-20 10:00:00",
        "CUSTOMER_ID": "C1",
    }
    graph = query_graph_with_groupby
    node = graph.get_node_by_name("groupby_1")
    preview_sql = get_forward_aggregate_sql(
        request_table_name=REQUEST_TABLE_NAME,
        graph=graph,
        nodes=[node],
        point_in_time_and_serving_name_list=[point_in_time_and_serving_name],
        source_type=SourceType.SNOWFLAKE,
    )

    assert_equal_with_expected_fixture(
        preview_sql, "tests/fixtures/expected_forward_aggregate.sql", update_fixture=update_fixtures
    )
