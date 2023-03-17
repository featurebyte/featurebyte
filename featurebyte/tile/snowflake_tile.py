"""
Snowflake Tile class
"""
from __future__ import annotations

from typing import Any, Optional

from datetime import datetime

import pandas as pd
from pydantic import PrivateAttr

from featurebyte.common import date_util
from featurebyte.enum import InternalName
from featurebyte.feature_manager.sql_template import tm_call_schedule_online_store
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.session.snowflake import SnowflakeSession
from featurebyte.tile.base import BaseTileManager
from featurebyte.tile.sql_template import (
    tm_generate_tile,
    tm_insert_tile_registry,
    tm_retrieve_tile_job_audit_logs,
    tm_select_tile_registry,
    tm_shell_schedule_tile,
    tm_tile_entity_tracking,
    tm_update_tile_registry,
)
from featurebyte.utils.snowflake.sql import escape_column_names


class TileManagerSnowflake(BaseTileManager):
    """
    Snowflake Tile class
    """

    _session: SnowflakeSession = PrivateAttr()
    _use_snowflake_scheduling: bool = PrivateAttr()

    def __init__(
        self,
        session: BaseSession,
        task_manager: Optional[TaskManager] = None,
        use_snowflake_scheduling: bool = True,
        **kw: Any,
    ) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        task_manager: Optional[TaskManager]
            input task manager
        use_snowflake_scheduling: bool
            whether to use snowflake scheduling mechanism
        kw: Any
            constructor arguments
        """
        super().__init__(session=session, task_manager=task_manager, **kw)
        self._use_snowflake_scheduling = use_snowflake_scheduling

    async def tile_job_exists(self, tile_spec: TileSpec) -> bool:
        """
        Get existing tile jobs for the given tile_spec

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec

        Returns
        -------
            whether the tile jobs already exist
        """
        exist_tasks = await self._session.execute_query(
            f"SHOW TASKS LIKE '%{tile_spec.aggregation_id}%'"
        )

        return exist_tasks is not None and len(exist_tasks) > 0

    async def populate_feature_store(self, tile_spec: TileSpec, job_schedule_ts_str: str) -> None:
        """
        Populate feature store with the given tile_spec and timestamp string

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        job_schedule_ts_str: str
            timestamp string of the job schedule
        """
        populate_sql = tm_call_schedule_online_store.render(
            aggregation_id=tile_spec.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
        )
        await self._session.execute_query(populate_sql)

    async def insert_tile_registry(self, tile_spec: TileSpec) -> bool:
        """
        Insert new tile registry record if it does not exist

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        Returns
        -------
            whether the tile registry record is inserted successfully or not
        """
        result = await self._session.execute_query(
            tm_select_tile_registry.render(tile_id=tile_spec.tile_id)
        )
        if result is None or len(result) == 0:
            sql = tm_insert_tile_registry.render(
                tile_id=tile_spec.tile_id,
                tile_sql=tile_spec.tile_sql,
                entity_column_names=",".join(escape_column_names(tile_spec.entity_column_names)),
                value_column_names=",".join(tile_spec.value_column_names),
                value_column_types=",".join(tile_spec.value_column_types),
                time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
                blind_spot_second=tile_spec.blind_spot_second,
                frequency_minute=tile_spec.frequency_minute,
            )
            logger.debug(f"generated tile insert sql: {sql}")
            await self._session.execute_query(sql)
            return True

        logger.warning(f"Tile id {tile_spec.tile_id} already exists")
        return False

    async def disable_tiles(self, tile_spec: TileSpec) -> None:
        """
        Disable tile jobs

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        """
        for t_type in TileType:
            tile_task_name = f"TILE_TASK_{t_type}_{tile_spec.tile_id}"
            await self._session.execute_query(f"ALTER TASK IF EXISTS {tile_task_name} SUSPEND")

        await self._session.execute_query(
            tm_update_tile_registry.render(tile_id=tile_spec.tile_id, is_enabled=False)
        )

    async def update_tile_entity_tracker(self, tile_spec: TileSpec, temp_entity_table: str) -> str:
        """
        Update <tile_id>_entity_tracker table for last_tile_start_date

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        temp_entity_table: str
            temporary entity table to be merge into <tile_id>_entity_tracker

        Returns
        -------
            generated sql
        """
        if tile_spec.category_column_name is None:
            entity_column_names = tile_spec.entity_column_names
        else:
            entity_column_names = [
                c for c in tile_spec.entity_column_names if c != tile_spec.category_column_name
            ]
        sql = tm_tile_entity_tracking.render(
            tile_id=tile_spec.aggregation_id,
            entity_column_names=",".join(escape_column_names(entity_column_names)),
            entity_table=temp_entity_table.replace("'", "''"),
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
        )
        await self._session.execute_query(sql)

        return sql

    async def generate_tiles(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        start_ts_str: Optional[str],
        end_ts_str: Optional[str],
        last_tile_start_ts_str: Optional[str] = None,
    ) -> str:
        """
        Manually trigger tile generation

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        tile_type: TileType
            tile type. ONLINE or OFFLINE
        start_ts_str: str
            start_timestamp of tile. ie. 2022-06-20 15:00:00
        end_ts_str: str
            end_timestamp of tile. ie. 2022-06-21 15:00:00
        last_tile_start_ts_str: str
            start date string of last tile used to update the tile_registry table

        Returns
        -------
            tile generation sql
        """
        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        tile_sql = tile_sql.replace("'", "''")

        if last_tile_start_ts_str:
            last_tile_start_ts_str = f"'{last_tile_start_ts_str}'"
        else:
            last_tile_start_ts_str = "null"

        logger.debug(f"last_tile_start_ts_str: {last_tile_start_ts_str}")

        sql = tm_generate_tile.render(
            tile_sql=tile_sql,
            tile_start_date_column=InternalName.TILE_START_DATE.value,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
            time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            entity_column_names=",".join(escape_column_names(tile_spec.entity_column_names)),
            value_column_names=",".join(tile_spec.value_column_names),
            value_column_types=",".join(tile_spec.value_column_types),
            tile_id=tile_spec.tile_id,
            tile_type=tile_type,
            last_tile_start_ts_str=last_tile_start_ts_str,
        )
        await self._session.execute_query(sql)

        return sql

    async def schedule_online_tiles(
        self,
        tile_spec: TileSpec,
        monitor_periods: int = 10,
        schedule_time: datetime = datetime.utcnow(),
    ) -> Optional[str]:
        """
        Schedule online tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        monitor_periods: int
            number of tile periods to monitor and re-generate. Default is 10
        schedule_time: datetime
            the moment of scheduling the job

        Returns
        -------
            generated sql to be executed
        """
        next_job_time = date_util.get_next_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=tile_spec.frequency_minute,
            time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
        )
        cron = f"{next_job_time.minute} {next_job_time.hour} {next_job_time.day} * *"

        if self._use_snowflake_scheduling:
            sql = await self._schedule_tiles(
                tile_spec=tile_spec,
                tile_type=TileType.ONLINE,
                cron_expr=cron,
                monitor_periods=monitor_periods,
            )
        else:
            sql = await super()._schedule_tiles_custom(
                tile_spec=tile_spec,
                tile_type=TileType.ONLINE,
                next_job_time=next_job_time,
                monitor_periods=monitor_periods,
            )

        return sql

    async def schedule_offline_tiles(
        self,
        tile_spec: TileSpec,
        offline_minutes: int = 1440,
        schedule_time: datetime = datetime.utcnow(),
    ) -> Optional[str]:
        """
        Schedule offline tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        offline_minutes: int
            offline tile lookback minutes to monitor and re-generate. Default is 1440
        schedule_time: datetime
            the moment of scheduling the job

        Returns
        -------
            generated sql to be executed
        """

        next_job_time = date_util.get_next_job_datetime(
            input_dt=schedule_time,
            frequency_minutes=offline_minutes,
            time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
        )
        cron = f"{next_job_time.minute} {next_job_time.hour} {next_job_time.day} * *"

        if self._use_snowflake_scheduling:
            sql = await self._schedule_tiles(
                tile_spec=tile_spec,
                tile_type=TileType.OFFLINE,
                cron_expr=cron,
                offline_minutes=offline_minutes,
            )
        else:
            sql = await super()._schedule_tiles_custom(
                tile_spec=tile_spec,
                tile_type=TileType.OFFLINE,
                next_job_time=next_job_time,
                offline_minutes=offline_minutes,
            )

        return sql

    async def _schedule_tiles(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        cron_expr: str,
        offline_minutes: int = 1440,
        monitor_periods: int = 10,
    ) -> Optional[str]:
        """
        Common tile schedule method

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        tile_type: TileType
            ONLINE or OFFLINE
        cron_expr: str
            cron expression for snowflake Task
        offline_minutes: int
            offline tile lookback minutes
        monitor_periods: int
            online tile lookback period

        Returns
        -------
            generated sql to be executed
        """

        temp_task_name = f"SHELL_TASK_{tile_spec.aggregation_id}_{tile_type}"

        # pylint: disable=duplicate-code
        sql = tm_shell_schedule_tile.render(
            temp_task_name=temp_task_name,
            warehouse=self._session.dict()["warehouse"],
            cron=cron_expr,
            tile_sql=tile_spec.tile_sql.replace("'", "''"),
            tile_start_date_column=InternalName.TILE_START_DATE.value,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
            tile_start_placeholder=InternalName.TILE_START_DATE_SQL_PLACEHOLDER.value,
            tile_end_placeholder=InternalName.TILE_END_DATE_SQL_PLACEHOLDER.value,
            time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            entity_column_names=",".join(escape_column_names(tile_spec.entity_column_names)),
            value_column_names=",".join(tile_spec.value_column_names),
            value_column_types=",".join(tile_spec.value_column_types),
            tile_id=tile_spec.tile_id,
            aggregation_id=tile_spec.aggregation_id,
            tile_type=tile_type,
            offline_minutes=offline_minutes,
            monitor_periods=monitor_periods,
        )

        await self._session.execute_query(sql)

        await self._session.execute_query(f"ALTER TASK IF EXISTS {temp_task_name} RESUME")

        return sql

    async def retrieve_tile_job_audit_logs(
        self,
        start_date: datetime,
        end_date: Optional[datetime] = None,
        tile_id: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Retrieve audit logs of tile job executions

        Parameters
        ----------
        start_date: datetime
            start date of retrieval
        end_date: Optional[datetime]
            end date of retrieval, optional
        tile_id: Optional[str]
            targeted tile id of retrieval, optional

        Returns
        -------
            dataframe of tile job execution information
        """
        date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
        start_date_str = start_date.strftime(date_format)
        end_date_str = end_date.strftime(date_format) if end_date else ""
        tile_id = tile_id if tile_id else ""

        sql = tm_retrieve_tile_job_audit_logs.render(
            start_date_str=start_date_str, end_date_str=end_date_str, tile_id=tile_id
        )
        result = await self._session.execute_query(sql)
        return result

    async def remove_tile_jobs(
        self,
        tile_spec: TileSpec,
    ) -> None:
        """
        Remove tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        """
        exist_mapping = await self._session.execute_query(
            f"SELECT * FROM TILE_FEATURE_MAPPING WHERE AGGREGATION_ID = '{tile_spec.aggregation_id}' and IS_DELETED = FALSE"
        )
        # only disable tile jobs when there is no tile-feature mapping records for the particular tile
        if exist_mapping is None or len(exist_mapping) == 0:
            if self._use_snowflake_scheduling:
                logger.info("Stopping job with Snowflake scheduler")
                exist_tasks = await self._session.execute_query(
                    f"SHOW TASKS LIKE '%{tile_spec.aggregation_id}%'"
                )
                if exist_tasks is not None and len(exist_tasks) > 0:
                    logger.warning(f"Start disabling jobs for {tile_spec.aggregation_id}")
                    for _, row in exist_tasks.iterrows():
                        await self._session.execute_query(f"DROP TASK IF EXISTS {row['name']}")
            else:
                await super().remove_tile_jobs(tile_spec=tile_spec)
