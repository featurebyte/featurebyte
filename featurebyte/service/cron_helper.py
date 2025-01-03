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
from sqlglot import expressions

from featurebyte.enum import InternalName, SpecialColumnName
from featurebyte.query_graph.model.feature_job_setting import CronFeatureJobSetting
from featurebyte.query_graph.sql.common import quoted_identifier, sql_to_string
from featurebyte.query_graph.sql.cron import (
    JobScheduleTable,
    JobScheduleTableSet,
)
from featurebyte.session.base import BaseSession

MAX_INTERVAL = relativedelta(months=1)


class CronHelper:
    """
    CronHelper class for helpers to simulate job schedules for historical features
    """

    @classmethod
    async def register_job_schedule_tables(
        cls,
        session: BaseSession,
        request_table_name: str,
        cron_feature_job_settings: list[CronFeatureJobSetting],
    ) -> JobScheduleTableSet:
        """
        Register job schedule tables for the given cron feature job settings. Caller is responsible
        for dropping the tables after use.

        Parameters
        ----------
        session: BaseSession
            Session object
        request_table_name: str
            Request table name
        cron_feature_job_settings: list[CronFeatureJobSetting]
            List of cron feature job settings

        Returns
        -------
        JobScheduleTableSet
        """
        point_in_time_min_max_query = expressions.select(
            expressions.alias_(
                expressions.Min(this=quoted_identifier(SpecialColumnName.POINT_IN_TIME)),
                alias="min",
                quoted=True,
            ),
            expressions.alias_(
                expressions.Max(this=quoted_identifier(SpecialColumnName.POINT_IN_TIME)),
                alias="max",
                quoted=True,
            ),
        ).from_(
            quoted_identifier(request_table_name),
        )
        point_in_time_stats = await session.execute_query_long_running(
            sql_to_string(point_in_time_min_max_query, session.source_type)
        )
        job_schedule_table_set = JobScheduleTableSet(tables=[])
        if point_in_time_stats is None:
            return job_schedule_table_set
        min_point_in_time = point_in_time_stats["min"].iloc[0]
        max_point_in_time = point_in_time_stats["max"].iloc[0]
        for cron_feature_job_setting in cron_feature_job_settings:
            job_schedule_table_name = f"__temp_cron_job_schedule_{ObjectId()}"
            await cls.register_cron_job_schedule(
                session=session,
                job_schedule_table_name=job_schedule_table_name,
                min_point_in_time=min_point_in_time,
                max_point_in_time=max_point_in_time,
                cron_feature_job_setting=cron_feature_job_setting,
            )
            job_schedule_table = JobScheduleTable(
                table_name=job_schedule_table_name,
                cron_feature_job_setting=cron_feature_job_setting,
            )
            job_schedule_table_set.tables.append(job_schedule_table)

        return job_schedule_table_set

    @classmethod
    def get_cron_job_schedule(
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
    async def register_cron_job_schedule(
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
        cron_job_schedule = cls.get_cron_job_schedule(
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
