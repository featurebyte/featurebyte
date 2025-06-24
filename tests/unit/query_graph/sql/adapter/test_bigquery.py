"""
Tests for BigQueryAdapter
"""

import textwrap

from sqlglot.expressions import select

from featurebyte.enum import SourceType
from featurebyte.query_graph.sql.adapter import BigQueryAdapter
from featurebyte.query_graph.sql.common import sql_to_string
from tests.unit.query_graph.sql.adapter.base_adapter_test import BaseAdapterTest
from tests.util.helper import assert_equal_with_expected_fixture, get_sql_adapter_from_source_type


class TestBigQueryAdapter(BaseAdapterTest):
    """
    Test bigquery adapter class
    """

    adapter = get_sql_adapter_from_source_type(SourceType.BIGQUERY)
    expected_physical_type_from_dtype_mapping = {
        "BOOL": "BOOLEAN",
        "CHAR": "STRING",
        "DATE": "DATE",
        "FLOAT": "FLOAT64",
        "INT": "FLOAT64",
        "TIME": "STRING",
        "TIMESTAMP": "TIMESTAMP",
        "TIMESTAMP_TZ": "TIMESTAMP",
        "VARCHAR": "STRING",
        "ARRAY": "ARRAY<FLOAT64>",
        "DICT": "JSON",
        "TIMEDELTA": "STRING",
        "EMBEDDING": "ARRAY<FLOAT64>",
        "FLAT_DICT": "JSON",
        "OBJECT": "JSON",
        "TIMESTAMP_TZ_TUPLE": "STRING",
        "UNKNOWN": "STRING",
        "BINARY": "STRING",
        "VOID": "STRING",
        "MAP": "JSON",
        "STRUCT": "JSON",
    }

    @classmethod
    def get_expected_haversine_sql(cls) -> str:
        """
        Get expected haversine SQL string
        """
        return textwrap.dedent(
            """
            2 * ASIN(
              SQRT(
                POWER(SIN((
                  ACOS(-1) * TABLE."lat1" / 180 - ACOS(-1) * TABLE."lat2" / 180
                ) / 2), 2) + COS(ACOS(-1) * TABLE."lat1" / 180) * COS(ACOS(-1) * TABLE."lat2" / 180) * POWER(SIN((
                  ACOS(-1) * TABLE."lon1" / 180 - ACOS(-1) * TABLE."lon2" / 180
                ) / 2), 2)
              )
            ) * 6371
        """
        ).strip()


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
