"""
Tests for BigQueryAdapter
"""

from sqlglot.expressions import select

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import BigQueryAdapter
from featurebyte.query_graph.sql.common import sql_to_string
from tests.util.helper import assert_equal_with_expected_fixture


def test_random_sample(update_fixtures):
    """
    Test random_sample
    """
    output = sql_to_string(
        BigQueryAdapter.random_sample(
            select("a", "b").from_("table1"),
            desired_row_count=100,
            total_row_count=1000,
            seed=1234,
        ),
        SourceType.BIGQUERY,
    )
    assert_equal_with_expected_fixture(
        output,
        "tests/fixtures/adapter/bigquery_random_sample.sql",
        update_fixtures,
    )


def test_random_sample_large_table(update_fixtures):
    """
    Test random_sample for large table
    """
    output = sql_to_string(
        BigQueryAdapter.random_sample(
            select("a", "b").from_("table1"),
            desired_row_count=100,
            total_row_count=10000001,
            seed=1234,
        ),
        SourceType.BIGQUERY,
    )
    assert_equal_with_expected_fixture(
        output,
        "tests/fixtures/adapter/bigquery_random_sample_large_table.sql",
        update_fixtures,
    )
