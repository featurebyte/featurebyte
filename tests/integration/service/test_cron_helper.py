"""
Integration tests for CronHelper
"""

from datetime import datetime

import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import expressions

from featurebyte import CronFeatureJobSetting
from featurebyte.enum import InternalName
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.cron import get_request_table_joined_job_schedule_expr
from featurebyte.service.cron_helper import CronHelper
from featurebyte.session.base import BaseSession
from tests.util.helper import fb_assert_frame_equal


async def register_request_table_with_job_schedule(
    cron_helper: CronHelper,
    session: BaseSession,
    request_table_name: str,
    request_table_columns: list[str],
    min_point_in_time: datetime,
    max_point_in_time: datetime,
    cron_feature_job_setting: CronFeatureJobSetting,
    output_table_name: str,
) -> None:
    """
    Test helper to register a new request table that has an additional column for the last cron job
    time corresponding to each point in time
    """
    job_schedule_table_name = f"__temp_cron_job_schedule_{ObjectId()}"
    try:
        await cron_helper.register_cron_job_schedule(
            session=session,
            job_schedule_table_name=job_schedule_table_name,
            min_point_in_time=min_point_in_time,
            max_point_in_time=max_point_in_time,
            cron_feature_job_setting=cron_feature_job_setting,
        )
        joined_expr = get_request_table_joined_job_schedule_expr(
            request_table_expr=expressions.select("*").from_(quoted_identifier(request_table_name)),
            request_table_columns=request_table_columns,
            job_schedule_table_name=job_schedule_table_name,
            job_datetime_output_column_name=InternalName.CRON_JOB_SCHEDULE_DATETIME,
            adapter=session.adapter,
        )
        await session.create_table_as(
            TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=output_table_name,
            ),
            joined_expr,
        )
    finally:
        await session.drop_table(
            table_name=job_schedule_table_name,
            schema_name=session.schema_name,
            database_name=session.database_name,
            if_exists=True,
        )


@pytest.fixture(name="cron_helper")
def cron_helper_fixture(app_container):
    """
    CronHelper fixture
    """
    return app_container.cron_helper


@pytest.mark.asyncio
async def test_register_request_table_with_job_schedule__utc_tz(
    session_without_datasets, cron_helper
):
    """
    Test registering a schedule table and a request table with schedule
    """
    session = session_without_datasets
    df_request_table = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime([
            "2022-05-15 10:00:00",
            "2022-06-15 10:00:00",
            "2022-07-15 10:00:00",
        ]),
        "SERIES_ID": [1, 2, 3],
    })
    request_table_name = f"request_table_{ObjectId()}"
    await session.register_table(request_table_name, df_request_table)

    output_table_name = f"output_table_{ObjectId()}"
    await register_request_table_with_job_schedule(
        cron_helper=cron_helper,
        session=session,
        request_table_name=request_table_name,
        request_table_columns=["POINT_IN_TIME", "SERIES_ID"],
        min_point_in_time=df_request_table["POINT_IN_TIME"].min(),
        max_point_in_time=df_request_table["POINT_IN_TIME"].max(),
        cron_feature_job_setting=CronFeatureJobSetting(
            crontab="0 10 * * *",
        ),
        output_table_name=output_table_name,
    )

    df = await session.execute_query(
        sql_to_string(
            expressions.select("*").from_(quoted_identifier(output_table_name)),
            session.source_type,
        ),
    )
    df_expected = df_request_table.copy()
    df_expected[InternalName.CRON_JOB_SCHEDULE_DATETIME] = pd.to_datetime([
        "2022-05-14 10:00:00",
        "2022-06-14 10:00:00",
        "2022-07-14 10:00:00",
    ])
    fb_assert_frame_equal(df, df_expected, sort_by_columns=["POINT_IN_TIME", "SERIES_ID"])


@pytest.mark.asyncio
async def test_register_request_table_with_job_schedule__non_utc_tz(
    session_without_datasets, cron_helper
):
    """
    Test registering a schedule table and a request table with schedule when the feature job
    setting's timezone is not UTC
    """
    session = session_without_datasets
    df_request_table = pd.DataFrame({
        "POINT_IN_TIME": pd.to_datetime([
            "2022-05-15 08:00:00",
            "2022-06-15 08:00:00",
            "2022-07-15 08:00:00",
        ]),
        "SERIES_ID": [1, 2, 3],
    })
    request_table_name = f"request_table_{ObjectId()}"
    await session.register_table(request_table_name, df_request_table)

    output_table_name = f"output_table_{ObjectId()}"
    await register_request_table_with_job_schedule(
        cron_helper=cron_helper,
        session=session,
        request_table_name=request_table_name,
        request_table_columns=["POINT_IN_TIME", "SERIES_ID"],
        min_point_in_time=df_request_table["POINT_IN_TIME"].min(),
        max_point_in_time=df_request_table["POINT_IN_TIME"].max(),
        cron_feature_job_setting=CronFeatureJobSetting(
            crontab="0 10 * * *",
            timezone="Asia/Tokyo",
        ),
        output_table_name=output_table_name,
    )

    df = await session.execute_query(
        sql_to_string(
            expressions.select("*").from_(quoted_identifier(output_table_name)),
            session.source_type,
        ),
    )
    df_expected = df_request_table.copy()
    # These are local times, e.g. "2022-05-15 10:00:00" in Asia/Tokyo is "2022-05-15 01:00:00" in
    # UTC, which is matched by the point in time of "2022-05-15 08:00:00" in UTC
    df_expected[InternalName.CRON_JOB_SCHEDULE_DATETIME] = pd.to_datetime([
        "2022-05-15 10:00:00",
        "2022-06-15 10:00:00",
        "2022-07-15 10:00:00",
    ])
    fb_assert_frame_equal(df, df_expected, sort_by_columns=["POINT_IN_TIME", "SERIES_ID"])
