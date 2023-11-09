"""
Unit tests for describe query
"""
import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize("source_type", [SourceType.SNOWFLAKE, SourceType.SPARK])
def test_graph_interpreter_describe(simple_graph, source_type, update_fixtures):
    """Test graph sample"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_type)

    sql_code = interpreter.construct_describe_sql(node.name, num_rows=10, seed=1234)[0]
    expected_filename = f"tests/fixtures/query_graph/expected_describe_{source_type.lower()}.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)


def test_describe_specify_stats_names(simple_graph, update_fixtures):
    """Test describe sql with only required stats names"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    sql_code, _, rows, columns = interpreter.construct_describe_sql(
        node.name, num_rows=10, seed=1234, stats_names=["min", "max"]
    )
    assert rows == ["dtype", "min", "max"]
    assert [column.name for column in columns] == ["ts", "cust_id", "a", "b", "a_copy"]
    expected_filename = f"tests/fixtures/query_graph/expected_describe_stats_names.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)


def test_describe_specify_count_based_stats_only(simple_graph, update_fixtures):
    """
    Test describe sql with only count based stats
    """
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    sql_code, _, rows, columns = interpreter.construct_describe_sql(
        node.name, num_rows=10, seed=1234, stats_names=["entropy"]
    )
    assert rows == ["dtype", "entropy"]
    assert [column.name for column in columns] == ["ts", "cust_id", "a", "b", "a_copy"]
    expected_filename = f"tests/fixtures/query_graph/expected_describe_count_based_stats_only.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)


def test_value_counts_sql(project_from_simple_graph, update_fixtures):
    """Test value counts sql"""
    graph, node = project_from_simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_value_counts_sql(
        node.name,
        num_rows=50000,
        num_categories_limit=1000,
    )
    expected_filename = f"tests/fixtures/query_graph/expected_value_counts.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)
