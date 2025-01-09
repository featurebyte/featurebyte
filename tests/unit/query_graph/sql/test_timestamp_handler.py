"""
Tests for query_graph/sql/timestamp_helper.py
"""

import pytest

from featurebyte.query_graph.model.timestamp_schema import TimestampSchema, TimeZoneColumn
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.timestamp_helper import convert_timestamp_to_utc
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
@pytest.mark.parametrize(
    "test_case_name,timestamp_schema",
    [
        (
            "varchar",
            TimestampSchema(format_string="%Y-%m-%d %H:%M:%S", timezone="Asia/Singapore"),
        ),
        (
            "varchar_utc",
            TimestampSchema(
                format_string="%Y-%m-%d %H:%M:%S", timezone="Asia/Singapore", is_utc_time=True
            ),
        ),
        (
            "varchar_tz_column_timezone",
            TimestampSchema(
                format_string="%Y-%m-%d %H:%M:%S",
                timezone=TimeZoneColumn(column_name="tz_col", type="timezone"),
            ),
        ),
        (
            "varchar_tz_column_offset",
            TimestampSchema(
                format_string="%Y-%m-%d %H:%M:%S",
                timezone=TimeZoneColumn(column_name="tz_col", type="offset"),
            ),
        ),
        ("timestamp", TimestampSchema(timezone="Asia/Singapore")),
        ("timestamp_utc", TimestampSchema(timezone="Asia/Singapore", is_utc_time=True)),
    ],
)
def test_convert_timestamp_to_utc(test_case_name, timestamp_schema, source_type, update_fixtures):
    """
    Test convert_timestamp_to_utc
    """
    conversion_expr = convert_timestamp_to_utc(
        column_expr=quoted_identifier("original_timestamp"),
        timestamp_schema=timestamp_schema,
        adapter=get_sql_adapter(
            SourceInfo(database_name="my_db", schema_name="my_schema", source_type=source_type)
        ),
    )
    actual = sql_to_string(conversion_expr, source_type)
    fixture_filename = (
        f"tests/fixtures/query_graph/test_timestamp_handler/{test_case_name}_{source_type}.sql"
    )
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)
