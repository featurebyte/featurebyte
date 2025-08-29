"""
Tests for query_graph/sql/timestamp_helper.py
"""

from typing import Literal

import pytest
from sqlglot import parse_one

from featurebyte.enum import DBVarType, TimeIntervalUnit
from featurebyte.models.periodic_task import Crontab
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.model.time_series_table import TimeInterval
from featurebyte.query_graph.model.timestamp_schema import (
    ExtendedTimestampSchema,
    TimestampSchema,
    TimestampTupleSchema,
    TimeZoneColumn,
    TimezoneOffsetSchema,
)
from featurebyte.query_graph.node.generic import (
    SnapshotsDatetimeJoinKey,
    SnapshotsDatetimeTransform,
)
from featurebyte.query_graph.sql.adapter import get_sql_adapter
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.source_info import SourceInfo
from featurebyte.query_graph.sql.timestamp_helper import (
    apply_snapshots_datetime_transform,
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


@pytest.mark.parametrize("source_type", SNOWFLAKE_SPARK_DATABRICKS_UNITY_BIGQUERY)
@pytest.mark.parametrize(
    "test_case_name,snapshots_datetime_join_key",
    [
        (
            "no_transform",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=None,
            ),
        ),
        (
            "basic_transform",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=SnapshotsDatetimeTransform(
                    original_timestamp_schema=None,
                    snapshot_timezone_name="America/New_York",
                    snapshot_time_interval=TimeInterval(unit=TimeIntervalUnit.HOUR, value=1),
                    snapshot_format_string=None,
                    snapshot_feature_job_setting=None,
                ),
            ),
        ),
        (
            "with_original_timestamp_schema",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=SnapshotsDatetimeTransform(
                    original_timestamp_schema=TimestampSchema(
                        timezone="America/Los_Angeles",
                        is_utc_time=False,
                    ),
                    snapshot_timezone_name="Asia/Singapore",
                    snapshot_time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
                    snapshot_format_string=None,
                    snapshot_feature_job_setting=None,
                ),
            ),
        ),
        (
            "with_format_string",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=SnapshotsDatetimeTransform(
                    original_timestamp_schema=TimestampSchema(
                        format_string="%Y-%m-%d %H:%M:%S",
                        timezone="Asia/Tokyo",
                        is_utc_time=True,
                    ),
                    snapshot_timezone_name="Europe/London",
                    snapshot_time_interval=TimeInterval(unit=TimeIntervalUnit.MINUTE, value=15),
                    snapshot_format_string="%Y-%m-%d %H:%M:%S",
                    snapshot_feature_job_setting=None,
                ),
            ),
        ),
        (
            "with_feature_job_setting_day_blind_spot",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=SnapshotsDatetimeTransform(
                    original_timestamp_schema=None,
                    snapshot_timezone_name="UTC",
                    snapshot_time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
                    snapshot_format_string=None,
                    snapshot_feature_job_setting=CronFeatureJobSetting(
                        crontab=Crontab(
                            minute=0, hour=0, day_of_month="*", month_of_year="*", day_of_week="*"
                        ),
                        timezone="UTC",
                        blind_spot="2d",
                    ),
                ),
            ),
        ),
        (
            "timezone_column_offset",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=SnapshotsDatetimeTransform(
                    original_timestamp_schema=TimestampSchema(
                        timezone=TimeZoneColumn(column_name="tz_offset", type="offset"),
                        is_utc_time=False,
                    ),
                    snapshot_timezone_name="UTC",
                    snapshot_time_interval=TimeInterval(unit=TimeIntervalUnit.HOUR, value=1),
                    snapshot_format_string=None,
                    snapshot_feature_job_setting=None,
                ),
            ),
        ),
        (
            "timezone_column_name",
            SnapshotsDatetimeJoinKey(
                column_name="event_timestamp",
                transform=SnapshotsDatetimeTransform(
                    original_timestamp_schema=TimestampSchema(
                        timezone=TimeZoneColumn(column_name="tz_name", type="timezone"),
                        is_utc_time=False,
                    ),
                    snapshot_timezone_name="America/Los_Angeles",
                    snapshot_time_interval=TimeInterval(unit=TimeIntervalUnit.DAY, value=1),
                    snapshot_format_string=None,
                    snapshot_feature_job_setting=None,
                ),
            ),
        ),
    ],
)
def test_apply_snapshots_datetime_transform(
    test_case_name, snapshots_datetime_join_key, source_type, update_fixtures
):
    """
    Test apply_snapshots_datetime_transform
    """
    # Create a sample table expression
    table_expr = parse_one(
        'SELECT "event_timestamp", "user_id", "amount" FROM events', read="snowflake"
    )
    adapter = get_sql_adapter(
        SourceInfo(database_name="my_db", schema_name="my_schema", source_type=source_type)
    )
    result_expr = apply_snapshots_datetime_transform(
        table_expr=table_expr,
        snapshots_datetime_join_key=snapshots_datetime_join_key,
        adapter=adapter,
    )
    actual = sql_to_string(result_expr, source_type)
    fixture_filename = f"tests/fixtures/query_graph/test_timestamp_handler/apply_snapshots_datetime_transform/{test_case_name}_{source_type}.sql"
    assert_equal_with_expected_fixture(actual, fixture_filename, update_fixtures)
