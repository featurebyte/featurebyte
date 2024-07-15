"""
Unit tests for describe query
"""

from datetime import datetime
from unittest.mock import patch

import pytest

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.interpreter import GraphInterpreter
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.fixture(autouse=True)
def patch_num_tables_per_join():
    """
    Patch NUM_TABLES_PER_JOIN to 2 for all tests
    """
    with patch("featurebyte.query_graph.sql.interpreter.preview.NUM_TABLES_PER_JOIN", 2):
        yield


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS)
def test_graph_interpreter_describe(simple_graph, source_type, update_fixtures):
    """Test graph sample"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, source_type)

    sql_code = (
        interpreter.construct_describe_queries(node.name, num_rows=10, seed=1234).queries[0].sql
    )
    expected_filename = f"tests/fixtures/query_graph/expected_describe_{source_type.lower()}.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)


def test_describe_specify_stats_names(simple_graph, update_fixtures):
    """Test describe sql with only required stats names"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    describe_query = interpreter.construct_describe_queries(
        node.name, num_rows=10, seed=1234, stats_names=["min", "max"]
    ).queries[0]
    assert describe_query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_stats_names.sql"
    assert_equal_with_expected_fixture(describe_query.sql, expected_filename, update_fixtures)


def test_describe_specify_count_based_stats_only(simple_graph, update_fixtures):
    """
    Test describe sql with only count based stats
    """
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    describe_query = interpreter.construct_describe_queries(
        node.name, num_rows=10, seed=1234, stats_names=["entropy"]
    ).queries[0]
    assert describe_query.row_names == ["dtype", "entropy"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_count_based_stats_only.sql"
    assert_equal_with_expected_fixture(describe_query.sql, expected_filename, update_fixtures)


def test_describe_specify_empty_stats(simple_graph, update_fixtures):
    """
    Test describe sql with empty stats edge case
    """
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    describe_query = interpreter.construct_describe_queries(
        node.name, num_rows=10, seed=1234, stats_names=[]
    ).queries[0]
    assert describe_query.row_names == ["dtype"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_empty_stats.sql"
    assert_equal_with_expected_fixture(describe_query.sql, expected_filename, update_fixtures)


def test_describe_in_batches(simple_graph, update_fixtures):
    """Test describe sql in batches"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    describe_queries = interpreter.construct_describe_queries(
        node.name,
        num_rows=10,
        seed=1234,
        stats_names=["min", "max"],
        columns_batch_size=3,
    )
    assert len(describe_queries.queries) == 2

    query = describe_queries.queries[0]
    assert query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in query.columns] == [
        "ts",
        "cust_id",
        "a",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_batches_0.sql"
    assert_equal_with_expected_fixture(query.sql, expected_filename, update_fixtures)

    query = describe_queries.queries[1]
    assert query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in query.columns] == [
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_batches_1.sql"
    assert_equal_with_expected_fixture(query.sql, expected_filename, update_fixtures)


def test_describe_no_batches(simple_graph, update_fixtures):
    """Test describe sql and disable batching by setting batch size to 0"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    describe_queries = interpreter.construct_describe_queries(
        node.name,
        num_rows=10,
        seed=1234,
        stats_names=["dtype", "entropy"],
        columns_batch_size=0,
    )
    assert len(describe_queries.queries) == 1
    describe_query = describe_queries.queries[0]
    assert describe_query.row_names == ["dtype", "entropy"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_count_based_stats_only.sql"
    assert_equal_with_expected_fixture(describe_query.sql, expected_filename, update_fixtures)


def test_describe_with_date_range_and_size(simple_graph, update_fixtures):
    """Test describe sql with only required stats names"""
    graph, node = simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)

    describe_query = interpreter.construct_describe_queries(
        node.name,
        num_rows=10,
        total_num_rows=100,
        from_timestamp=datetime(2024, 1, 1),
        to_timestamp=datetime(2024, 2, 1),
        seed=1234,
        stats_names=["min", "max"],
    ).queries[0]
    assert describe_query.row_names == ["dtype", "min", "max"]
    assert [column.name for column in describe_query.columns] == [
        "ts",
        "cust_id",
        "a",
        "b",
        "a_copy",
    ]
    expected_filename = "tests/fixtures/query_graph/expected_describe_date_range_and_size.sql"
    assert_equal_with_expected_fixture(describe_query.sql, expected_filename, update_fixtures)


def test_value_counts_sql(project_from_simple_graph, update_fixtures):
    """Test value counts sql"""
    graph, node = project_from_simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_value_counts_sql(
        node.name,
        num_rows=50000,
        num_categories_limit=1000,
    )
    expected_filename = "tests/fixtures/query_graph/expected_value_counts.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)


def test_value_counts_sql_no_casting(project_from_simple_graph, update_fixtures):
    """Test value counts sql"""
    graph, node = project_from_simple_graph
    interpreter = GraphInterpreter(graph, SourceType.SNOWFLAKE)
    sql_code = interpreter.construct_value_counts_sql(
        node.name,
        num_rows=50000,
        num_categories_limit=1000,
        convert_keys_to_string=False,
    )
    expected_filename = "tests/fixtures/query_graph/expected_value_counts_no_casting.sql"
    assert_equal_with_expected_fixture(sql_code, expected_filename, update_fixtures)
