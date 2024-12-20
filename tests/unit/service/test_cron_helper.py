"""
Tests for cron_helper.py
"""

from datetime import datetime

import pytest

from featurebyte import CronFeatureJobSetting
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
    datetimes = cron_helper.get_cron_job_schedule(
        min_point_in_time=datetime(2024, 1, 15, 10, 0, 0),
        max_point_in_time=datetime(2024, 2, 15, 10, 0, 0),
        cron_feature_job_setting=feature_job_setting,
    )
    assert datetimes == [
        datetime(2023, 12, 18, 10, 0),
        datetime(2023, 12, 25, 10, 0),
        datetime(2024, 1, 1, 10, 0),
        datetime(2024, 1, 8, 10, 0),
        datetime(2024, 1, 15, 10, 0),
        datetime(2024, 1, 22, 10, 0),
        datetime(2024, 1, 29, 10, 0),
        datetime(2024, 2, 5, 10, 0),
        datetime(2024, 2, 12, 10, 0),
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
        job_schedule_table_name="cron_schedule_1",
        output_table_name="request_table_cron_schedule_1",
    )
    query = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        query,
        "tests/fixtures/cron_helper/test_register_request_table_with_job_schedule.sql",
        update_fixtures,
    )
