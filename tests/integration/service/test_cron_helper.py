"""
Integration tests for CronHelper
"""

import pandas as pd
import pytest
from bson import ObjectId
from sqlglot import expressions

from featurebyte import CronFeatureJobSetting
from featurebyte.enum import InternalName
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from tests.util.helper import fb_assert_frame_equal


@pytest.fixture(name="cron_helper")
def cron_helper_fixture(app_container):
    """
    CronHelper fixture
    """
    return app_container.cron_helper


@pytest.mark.asyncio
async def test_register_request_table_with_job_schedule(session_without_datasets, cron_helper):
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
    await cron_helper.register_request_table_with_job_schedule(
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
