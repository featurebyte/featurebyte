"""
Helpers to simulate job schedules for historical features
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
from croniter import croniter_range
from dateutil.relativedelta import relativedelta

from featurebyte import CronFeatureJobSetting
from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.query_graph.sql.common import quoted_identifier
from featurebyte.query_graph.sql.scd_helper import Table, get_scd_join_expr
from featurebyte.session.base import BaseSession

MAX_INTERVAL = relativedelta(months=1)


class CronHelper:
    """
    CronHelper class for helpers to simulate job schedules for historical features
    """

    @classmethod
    def get_cron_job_schedule(
        cls,
        min_point_in_time: datetime,
        max_point_in_time: datetime,
        cron_feature_job_setting: CronFeatureJobSetting,
    ) -> list[datetime]:
        """
        Get the schedule for the cron job based on the min and max point in time
        """
        start = min_point_in_time - MAX_INTERVAL
        return list(
            croniter_range(start, max_point_in_time, cron_feature_job_setting.get_cron_expression())
        )

    @classmethod
    async def register_cron_job_schedule(
        cls,
        session: BaseSession,
        job_schedule_table_name: str,
        min_point_in_time: datetime,
        max_point_in_time: datetime,
        cron_feature_job_setting: CronFeatureJobSetting,
    ) -> None:
        """
        Register the cron job schedule based on the min and max point in time
        """
        cron_job_schedule = cls.get_cron_job_schedule(
            min_point_in_time, max_point_in_time, cron_feature_job_setting
        )
        df_cron_job_schedule = pd.DataFrame({
            InternalName.CRON_JOB_SCHEDULE_DATETIME: cron_job_schedule
        })
        await session.register_table(job_schedule_table_name, df_cron_job_schedule)
        return cron_job_schedule

    @classmethod
    async def register_request_table_with_job_schedule(
        cls,
        session: BaseSession,
        request_table_name: str,
        request_table_columns: list[str],
        job_schedule_table_name: str,
        output_table_name: str,
    ) -> None:
        """
        Register a new request table by joining the original request table with the cron job
        schedule table
        """
        left_table = Table(
            expr=quoted_identifier(request_table_name),
            timestamp_column=SpecialColumnName.POINT_IN_TIME,
            join_keys=[],
            input_columns=request_table_columns,
            output_columns=request_table_columns,
        )
        right_table = Table(
            expr=quoted_identifier(job_schedule_table_name),
            timestamp_column=InternalName.CRON_JOB_SCHEDULE_DATETIME,
            join_keys=[],
            input_columns=[InternalName.CRON_JOB_SCHEDULE_DATETIME],
            output_columns=[InternalName.CRON_JOB_SCHEDULE_DATETIME],
        )
        joined_expr = get_scd_join_expr(
            left_table=left_table,
            right_table=right_table,
            join_type="left",
            adapter=session.adapter,
            allow_exact_match=False,
        )
        await session.create_table_as(
            TableDetails(
                database_name=session.database_name,
                schema_name=session.schema_name,
                table_name=output_table_name,
            ),
            joined_expr,
        )
