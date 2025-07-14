"""
Tests for query_graph/sql/partition_filter.py
"""

from datetime import datetime

import pytest

from featurebyte import SourceType, TimeInterval, TimeIntervalUnit
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import PartitionColumnFilter, sql_to_string
from featurebyte.query_graph.sql.partition_filter import get_partition_filter
from featurebyte.query_graph.sql.source_info import SourceInfo
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize(
    "source_type", [SourceType.SNOWFLAKE, SourceType.DATABRICKS_UNITY, SourceType.BIGQUERY]
)
@pytest.mark.parametrize(
    "test_case, from_timestamp, to_timestamp, format_string, buffer",
    [
        ("timestamp", datetime(2023, 2, 1, 0, 0, 0), datetime(2023, 5, 1, 0, 0, 0), None, None),
        (
            "varchar",
            datetime(2023, 2, 1, 0, 0, 0),
            datetime(2023, 5, 1, 0, 0, 0),
            "yyyy-MM-dd",
            None,
        ),
        ("from_only", datetime(2023, 2, 1, 0, 0, 0), None, None, None),
        ("to_only", None, datetime(2023, 5, 1, 0, 0, 0), None, None),
        (
            "varchar_2d",
            datetime(2023, 2, 1, 0, 0, 0),
            datetime(2023, 5, 1, 0, 0, 0),
            "yyyy-MM-dd",
            TimeInterval(unit=TimeIntervalUnit.DAY, value=2),
        ),
        (
            "varchar_2m",
            datetime(2023, 2, 1, 0, 0, 0),
            datetime(2023, 5, 1, 0, 0, 0),
            "yyyy-MM-dd",
            TimeInterval(unit=TimeIntervalUnit.MONTH, value=2),
        ),
    ],
)
def test_get_partition_filter(
    test_case, from_timestamp, to_timestamp, format_string, buffer, source_type, update_fixtures
):
    """
    Test get_partition_filter
    """
    partition_column_filter = PartitionColumnFilter(
        from_timestamp=from_timestamp,
        to_timestamp=to_timestamp,
        buffer=buffer,
    )
    expr = get_partition_filter(
        partition_column="partition_col",
        partition_column_filter=partition_column_filter,
        format_string=format_string,
        adapter=get_sql_adapter(
            source_info=SourceInfo(
                database_name="db",
                schema_name="db",
                source_type=source_type,
            )
        ),
    )
    fixture_filename = (
        f"tests/fixtures/query_graph/test_partition_filter/{test_case}_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(
        sql_to_string(expr, source_type),
        fixture_filename,
        update_fixtures,
    )
