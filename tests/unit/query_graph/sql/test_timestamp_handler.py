"""
Tests for query_graph/sql/timestamp_helper.py
"""

from typing import Literal

import pytest

from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.timestamp_schema import (
    ExtendedTimestampSchema,
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
    TimezoneOffsetSchema,
)
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.timestamp_helper import (
    convert_timestamp_timezone_tuple,
    convert_timestamp_to_local,
    convert_timestamp_to_utc,
)
from tests.source_types import SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY
from tests.util.helper import assert_equal_with_expected_fixture


@pytest.mark.parametrize("method", ["convert_timestamp_to_utc", "convert_timestamp_to_local"])
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
        (
            "varchar_tz_column_offset_utc",
            TimestampSchema(
                format_string="%Y-%m-%d %H:%M:%S",
                timezone=TimeZoneColumn(column_name="tz_col", type="offset"),
                is_utc_time=True,
            ),
        ),
        ("timestamp", TimestampSchema(timezone="Asia/Singapore")),
        ("timestamp_utc", TimestampSchema(timezone="Asia/Singapore", is_utc_time=True)),
    ],
)
def test_convert_timestamp(method, test_case_name, timestamp_schema, source_type, update_fixtures):
    """
    Test convert_timestamp_to_utc and convert_timestamp_to_local
    """
    if method == "convert_timestamp_to_utc":
        func = convert_timestamp_to_utc
    else:
        assert method == "convert_timestamp_to_local"
        func = convert_timestamp_to_local
    conversion_expr = func(
        column_expr=quoted_identifier("original_timestamp"),
        timestamp_schema=timestamp_schema,
        adapter=get_sql_adapter(
            SourceInfo(database_name="my_db", schema_name="my_schema", source_type=source_type)
        ),
    )
    actual = sql_to_string(conversion_expr, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_timestamp_handler/{method}/{test_case_name}_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
@pytest.mark.parametrize("target_tz", ["utc", "local"])
@pytest.mark.parametrize(
    "test_case_name,timestamp_tuple_schema",
    [
        (
            "varchar_tz_column_timezone",
            TimestampTupleSchema(
                timestamp_schema=ExtendedTimestampSchema(
                    dtype=DBVarType.VARCHAR,
                    format_string="%Y-%m-%d %H:%M:%S",
                    timezone=TimeZoneColumn(column_name="tz_col", type="timezone"),
                ),
                timezone_offset_schema=TimezoneOffsetSchema(dtype=DBVarType.VARCHAR),
            ),
        ),
        (
            "varchar_tz_column_offset",
            TimestampTupleSchema(
                timestamp_schema=ExtendedTimestampSchema(
                    dtype=DBVarType.VARCHAR,
                    format_string="%Y-%m-%d %H:%M:%S",
                    timezone=TimeZoneColumn(column_name="tz_col", type="offset"),
                ),
                timezone_offset_schema=TimezoneOffsetSchema(dtype=DBVarType.VARCHAR),
            ),
        ),
    ],
)
def test_convert_timestamp_timezone_tuple(
    test_case_name, timestamp_tuple_schema, target_tz, source_type, update_fixtures
):
    """
    Test convert_timestamp_timezone_tuple
    """
    target_tz: Literal["utc", "local"]
    conversion_expr = convert_timestamp_timezone_tuple(
        zipped_expr=quoted_identifier("zipped_timestamp_tuple"),
        target_tz=target_tz,
        timestamp_tuple_schema=timestamp_tuple_schema,
        adapter=get_sql_adapter(
            SourceInfo(database_name="my_db", schema_name="my_schema", source_type=source_type)
        ),
    )
    actual = sql_to_string(conversion_expr, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_timestamp_handler/convert_timestamp_timezone_tuple/{target_tz}/{test_case_name}_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)
