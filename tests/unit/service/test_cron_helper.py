"""
Tests for cron_helper.py
"""

from datetime import datetime

import pandas as pd
import pytest
import pytz
from pandas import Timestamp

from featurebyte import CronFeatureJobSetting, Crontab
from featurebyte.enum import InternalName
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.cron import (
    JobScheduleTable,
    JobScheduleTableSet,
    get_request_table_joined_job_schedule_expr,
)
from tests.util.helper import (
    assert_equal_with_expected_fixture,
    assert_sql_equal,
    extract_session_executed_queries,
)


@pytest.fixture(name="cron_helper")
def cron_helper_fixture(app_container):
    """
    CronHelper fixture
    """
    return app_container.cron_helper


@pytest.mark.parametrize("point_in_time_has_tz", [False, True])
def test_get_cron_schedule(cron_helper, point_in_time_has_tz):
    """
    Test get_cron_schedule
    """
    feature_job_setting = CronFeatureJobSetting(
        crontab="0 10 * * 1",
    )
    if point_in_time_has_tz:
        min_point_in_time = datetime(2024, 1, 15, 10, 0, 0, tzinfo=pytz.utc)
        max_point_in_time = datetime(2024, 2, 15, 10, 0, 0, tzinfo=pytz.utc)
    else:
        min_point_in_time = datetime(2024, 1, 15, 10, 0, 0)
        max_point_in_time = datetime(2024, 2, 15, 10, 0, 0)
    datetimes = cron_helper.get_cron_job_schedule(
        min_point_in_time=min_point_in_time,
        max_point_in_time=max_point_in_time,
        cron_feature_job_setting=feature_job_setting,
    )
    tzinfo = pytz.timezone("Etc/UTC")
    assert datetimes == [
        datetime(2024, 1, 1, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 8, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 15, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 22, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 29, 10, 0, tzinfo=tzinfo),
        datetime(2024, 2, 5, 10, 0, tzinfo=tzinfo),
        datetime(2024, 2, 12, 10, 0, tzinfo=tzinfo),
    ]


@pytest.mark.asyncio
async def test_register_job_schedule_tables(cron_helper, mock_snowflake_session):
    """
    Test register_job_schedule_tables
    """
    cron_feature_job_settings = [
        CronFeatureJobSetting(crontab="0 10 * * 1"),
        CronFeatureJobSetting(crontab="30 8 * * *"),
    ]
    mock_snowflake_session.execute_query_long_running.return_value = pd.DataFrame({
        "min": [pd.Timestamp("2024-01-15 10:00:00")],
        "max": [pd.Timestamp("2024-02-25 10:00:00")],
    })
    job_schedule_table_set = await cron_helper.register_job_schedule_tables(
        session=mock_snowflake_session,
        request_table_name="request_table",
        cron_feature_job_settings=cron_feature_job_settings,
    )
    assert job_schedule_table_set == JobScheduleTableSet(
        tables=[
            JobScheduleTable(
                table_name="__temp_cron_job_schedule_000000000000000000000000",
                cron_feature_job_setting=CronFeatureJobSetting(
                    crontab=Crontab(
                        minute=0, hour=10, day_of_month="*", month_of_year="*", day_of_week=1
                    ),
                    timezone="Etc/UTC",
                ),
            ),
            JobScheduleTable(
                table_name="__temp_cron_job_schedule_000000000000000000000001",
                cron_feature_job_setting=CronFeatureJobSetting(
                    crontab=Crontab(
                        minute=30, hour=8, day_of_month="*", month_of_year="*", day_of_week="*"
                    ),
                    timezone="Etc/UTC",
                ),
            ),
        ]
    )
    query = extract_session_executed_queries(mock_snowflake_session)
    assert_sql_equal(
        query,
        """
        SELECT
          MIN("POINT_IN_TIME") AS "min",
          MAX("POINT_IN_TIME") AS "max"
        FROM "request_table";
        """,
    )


@pytest.mark.parametrize("reference_timezone", [None, "Asia/Singapore"])
@pytest.mark.asyncio
async def test_register_request_table_with_job_schedule(
    reference_timezone, cron_helper, mock_snowflake_session, update_fixtures
):
    """
    Test register_request_table_with_job_schedule
    """
    session = mock_snowflake_session

    # Register job schedule table
    job_schedule_table_name = "__temp_cron_job_schedule_000000000000000000000000"
    await cron_helper.register_cron_job_schedule(
        session=session,
        job_schedule_table_name=job_schedule_table_name,
        min_point_in_time=datetime(2024, 1, 15, 10, 0, 0),
        max_point_in_time=datetime(2024, 2, 15, 10, 0, 0),
        cron_feature_job_setting=CronFeatureJobSetting(
            crontab="0 10 * * 1",
            timezone="Asia/Tokyo",
            reference_timezone=reference_timezone,
        ),
    )

    # Register a request table joined with schedule table
    joined_expr = get_request_table_joined_job_schedule_expr(
        request_table_expr=quoted_identifier("request_table"),
        request_table_columns=["POINT_IN_TIME", "SERIES_ID"],
        job_schedule_table_name=job_schedule_table_name,
        job_datetime_output_column_name=InternalName.CRON_JOB_SCHEDULE_DATETIME,
        adapter=session.adapter,
    )
    await session.create_table_as(
        TableDetails(
            database_name=session.database_name,
            schema_name=session.schema_name,
            table_name="request_table_cron_schedule_1",
        ),
        joined_expr,
    )

    # Check executed queries
    query = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        query,
        "tests/fixtures/cron_helper/test_register_request_table_with_job_schedule.sql",
        update_fixtures,
    )

    # Check temporary schedule table registered
    args, _ = mock_snowflake_session.register_table.call_args
    df_schedule = args[1]
    assert df_schedule.columns.to_list() == [
        InternalName.CRON_JOB_SCHEDULE_DATETIME,
        InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC,
    ]
    if reference_timezone is None:
        expected_schedules = [
            Timestamp("2024-01-08 10:00:00"),
            Timestamp("2024-01-15 10:00:00"),
            Timestamp("2024-01-22 10:00:00"),
            Timestamp("2024-01-29 10:00:00"),
            Timestamp("2024-02-05 10:00:00"),
            Timestamp("2024-02-12 10:00:00"),
        ]
    else:
        assert reference_timezone == "Asia/Singapore"
        expected_schedules = [
            Timestamp("2024-01-08 09:00:00"),
            Timestamp("2024-01-15 09:00:00"),
            Timestamp("2024-01-22 09:00:00"),
            Timestamp("2024-01-29 09:00:00"),
            Timestamp("2024-02-05 09:00:00"),
            Timestamp("2024-02-12 09:00:00"),
        ]
    assert df_schedule[InternalName.CRON_JOB_SCHEDULE_DATETIME].to_list() == expected_schedules
    assert df_schedule[InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC].to_list() == [
        Timestamp("2024-01-08 01:00:00"),
        Timestamp("2024-01-15 01:00:00"),
        Timestamp("2024-01-22 01:00:00"),
        Timestamp("2024-01-29 01:00:00"),
        Timestamp("2024-02-05 01:00:00"),
        Timestamp("2024-02-12 01:00:00"),
    ]


@pytest.mark.parametrize(
    "cron_expr, timezone, current_time, expected_next_time",
    [
        (
            "0 10 * * 1",
            "Etc/UTC",
            datetime(2024, 1, 15, 9, 0, 0),
            datetime(2024, 1, 15, 10, 0, 0),
        ),
        (
            "0 10 * * *",
            "Asia/Singapore",
            datetime(2024, 1, 15, 9, 0, 0),
            datetime(2024, 1, 16, 2, 0, 0),
        ),
    ],
)
def test_get_next_scheduled_job_ts(
    cron_helper, cron_expr, timezone, current_time, expected_next_time
):
    """Test get_next_scheduled_job_ts"""
    feature_job_setting = CronFeatureJobSetting(crontab=cron_expr, timezone=timezone)
    next_time = cron_helper.get_next_scheduled_job_ts(
        cron_feature_job_setting=feature_job_setting, current_ts=current_time
    )
    assert next_time == expected_next_time
