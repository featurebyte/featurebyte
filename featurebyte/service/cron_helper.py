"""
Helpers to simulate job schedules for historical features
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytz
from bson import ObjectId
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
    async def register_request_table_with_job_schedule(
        cls,
        session: BaseSession,
        request_table_name: str,
        request_table_columns: list[str],
        min_point_in_time: datetime,
        max_point_in_time: datetime,
        cron_feature_job_setting: CronFeatureJobSetting,
        output_table_name: str,
    ) -> None:
        """
        Register a new request table that has an additional column for the last cron job time
        corresponding to each point in time

        Parameters
        ----------
        session: BaseSession
            Session object
        request_table_name: str
            Request table name
        request_table_columns: list[str]
            Request table columns
        min_point_in_time: datetime
            Minimum point in time used to determine the range of cron job schedule
        max_point_in_time: datetime
            Maximum point in time used to determine the range of cron job schedule
        cron_feature_job_setting: CronFeatureJobSetting
            Cron feature job setting to simulate
        output_table_name: str
            Output table name of the request table with the cron job schedule
        """
        job_schedule_table_name = f"__temp_cron_job_schedule_{ObjectId()}"
        try:
            await cls._register_cron_job_schedule(
                session=session,
                job_schedule_table_name=job_schedule_table_name,
                min_point_in_time=min_point_in_time,
                max_point_in_time=max_point_in_time,
                cron_feature_job_setting=cron_feature_job_setting,
            )
            await cls._join_request_table_with_job_schedule(
                session=session,
                request_table_name=request_table_name,
                request_table_columns=request_table_columns,
                job_schedule_table_name=job_schedule_table_name,
                output_table_name=output_table_name,
            )
        finally:
            await session.drop_table(
                table_name=job_schedule_table_name,
                schema_name=session.schema_name,
                database_name=session.database_name,
                if_exists=True,
            )

    @classmethod
    def _get_cron_job_schedule(
        cls,
        min_point_in_time: datetime,
        max_point_in_time: datetime,
        cron_feature_job_setting: CronFeatureJobSetting,
    ) -> list[datetime]:
        """
        Get the schedule for the cron job based on the min and max point in time

        Parameters
        ----------
        min_point_in_time: datetime
            Minimum point in time used to determine the range of cron job schedule
        max_point_in_time: datetime
            Maximum point in time used to determine the range of cron job schedule
        cron_feature_job_setting: CronFeatureJobSetting
            Cron feature job setting to simulate

        Returns
        -------
        list[datetime]
        """
        start = pytz.utc.localize(min_point_in_time - MAX_INTERVAL)
        end = pytz.utc.localize(max_point_in_time)
        tz = pytz.timezone(cron_feature_job_setting.timezone)
        start_local = start.astimezone(tz)
        end_local = end.astimezone(tz)
        return list(
            croniter_range(start_local, end_local, cron_feature_job_setting.get_cron_expression())
        )

    @classmethod
    async def _register_cron_job_schedule(
        cls,
        session: BaseSession,
        job_schedule_table_name: str,
        min_point_in_time: datetime,
        max_point_in_time: datetime,
        cron_feature_job_setting: CronFeatureJobSetting,
    ) -> None:
        """
        Register a table containing the cron job schedule based on the min and max point in time

        Parameters
        ----------
        session: BaseSession
            Session object
        job_schedule_table_name: str
            Job schedule table name
        min_point_in_time: datetime
            Minimum point in time used to determine the range of cron job schedule
        max_point_in_time: datetime
            Maximum point in time used to determine the range of cron job schedule
        cron_feature_job_setting: CronFeatureJobSetting
            Cron feature job setting to simulate
        """
        cron_job_schedule = cls._get_cron_job_schedule(
            min_point_in_time, max_point_in_time, cron_feature_job_setting
        )
        cron_job_schedule_utc = [dt.astimezone(pytz.utc) for dt in cron_job_schedule]
        df_cron_job_schedule = pd.DataFrame({
            InternalName.CRON_JOB_SCHEDULE_DATETIME: [
                dt.replace(tzinfo=None) for dt in cron_job_schedule
            ],
            InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC: [
                dt.replace(tzinfo=None) for dt in cron_job_schedule_utc
            ],
        })
        await session.register_table(job_schedule_table_name, df_cron_job_schedule)

    @classmethod
    async def _join_request_table_with_job_schedule(
        cls,
        session: BaseSession,
        request_table_name: str,
        request_table_columns: list[str],
        job_schedule_table_name: str,
        output_table_name: str,
    ) -> None:
        """
        Register a new request table by joining each row in the request table to the latest row in
        the cron job schedule table where the job datetime is before the point in time.

        Parameters
        ----------
        session: BaseSession
            Session object
        request_table_name: str
            Request table name
        request_table_columns: list[str]
            Request table columns
        job_schedule_table_name: str
            Job schedule table name
        output_table_name: str
            Output table name of the request table with the cron job schedule
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
            timestamp_column=InternalName.CRON_JOB_SCHEDULE_DATETIME_UTC,
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
