"""
Tests for cron_helper.py
"""

from datetime import datetime
from unittest.mock import call

import pytest
import pytz
from pandas import Timestamp

from featurebyte import CronFeatureJobSetting
from featurebyte.enum import InternalName
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


@pytest.fixture(name="cron_helper")
def cron_helper_fixture(app_container):
    """
    CronHelper fixture
    """
    return app_container.cron_helper


def test_get_cron_schedule(cron_helper):
    """
    Test get_cron_schedule
    """
    feature_job_setting = CronFeatureJobSetting(
        crontab="0 10 * * 1",
    )
    datetimes = cron_helper._get_cron_job_schedule(
        min_point_in_time=datetime(2024, 1, 15, 10, 0, 0),
        max_point_in_time=datetime(2024, 2, 15, 10, 0, 0),
        cron_feature_job_setting=feature_job_setting,
    )
    tzinfo = pytz.timezone("Etc/UTC")
    assert datetimes == [
        datetime(2023, 12, 18, 10, 0, tzinfo=tzinfo),
        datetime(2023, 12, 25, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 1, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 8, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 15, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 22, 10, 0, tzinfo=tzinfo),
        datetime(2024, 1, 29, 10, 0, tzinfo=tzinfo),
        datetime(2024, 2, 5, 10, 0, tzinfo=tzinfo),
        datetime(2024, 2, 12, 10, 0, tzinfo=tzinfo),
    ]


@pytest.mark.asyncio
async def test_register_request_table_with_job_schedule(
    cron_helper, mock_snowflake_session, update_fixtures
):
    """
    Test register_request_table_with_job_schedule
    """
    await cron_helper.register_request_table_with_job_schedule(
        session=mock_snowflake_session,
        request_table_name="request_table",
        request_table_columns=["POINT_IN_TIME", "SERIES_ID"],
        min_point_in_time=datetime(2024, 1, 15, 10, 0, 0),
        max_point_in_time=datetime(2024, 2, 15, 10, 0, 0),
        cron_feature_job_setting=CronFeatureJobSetting(
            crontab="0 10 * * 1",
            timezone="Asia/Tokyo",
        ),
        output_table_name="request_table_cron_schedule_1",
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
    assert df_schedule[InternalName.CRON_JOB_SCHEDULE_DATETIME].to_list() == [
        Timestamp("2023-12-18 10:00:00"),
        Timestamp("2023-12-25 10:00:00"),
        Timestamp("2024-01-01 10:00:00"),
        Timestamp("2024-01-08 10:00:00"),
        Timestamp("2024-01-15 10:00:00"),
        Timestamp("2024-01-22 10:00:00"),
        Timestamp("2024-01-29 10:00:00"),
        Timestamp("2024-02-05 10:00:00"),
        Timestamp("2024-02-12 10:00:00"),
    ]
    assert df_schedule[InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC].to_list() == [
        Timestamp("2023-12-18 01:00:00"),
        Timestamp("2023-12-25 01:00:00"),
        Timestamp("2024-01-01 01:00:00"),
        Timestamp("2024-01-08 01:00:00"),
        Timestamp("2024-01-15 01:00:00"),
        Timestamp("2024-01-22 01:00:00"),
        Timestamp("2024-01-29 01:00:00"),
        Timestamp("2024-02-05 01:00:00"),
        Timestamp("2024-02-12 01:00:00"),
    ]

    # Check temporary schedule table dropped
    assert mock_snowflake_session.drop_table.call_args_list == [
        call(
            table_name=f'__temp_cron_job_schedule_{"0" * 24}',
            schema_name=mock_snowflake_session.schema_name,
            database_name=mock_snowflake_session.database_name,
            if_exists=True,
        )
    ]
