"""
Base Tile class
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

from datetime import datetime

import numpy as np
import pandas as pd
from pydantic import BaseModel, PrivateAttr

from featurebyte.enum import InternalName
from featurebyte.exception import TileScheduleNotSupportedError
from featurebyte.logging import get_logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.sql.tile_generate import TileGenerate
from featurebyte.sql.tile_generate_entity_tracking import TileGenerateEntityTracking
from featurebyte.sql.tile_generate_schedule import TileGenerateSchedule
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore
from featurebyte.tile.scheduler import TileScheduler
from featurebyte.tile.sql_template import tm_retrieve_tile_job_audit_logs

TILE_COMPUTE_PROGRESS_MAX_PERCENT = 90  #  Progress percentage to report at end of tile computation
logger = get_logger(__name__)


class TileManager(BaseModel):
    """
    Base Tile class
    """

    _session: BaseSession = PrivateAttr()
    _task_manager: Optional[TaskManager] = PrivateAttr()

    def __init__(
        self, session: BaseSession, task_manager: Optional[TaskManager] = None, **kw: Any
    ) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        task_manager: Optional[TaskManager]
            input task manager
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session
        self._task_manager = task_manager

    async def generate_tiles_on_demand(
        self,
        tile_inputs: List[Tuple[TileSpec, str]],
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        progress_callback: Optional[Callable[[int, str], None]]
            Optional progress callback function
        """
        num_jobs = len(tile_inputs)
        if progress_callback:
            progress_callback(0, f"0/{num_jobs} completed")

        for index, (tile_spec, entity_table) in enumerate(tile_inputs):
            await self.generate_tiles(
                tile_spec=tile_spec, tile_type=TileType.OFFLINE, start_ts_str=None, end_ts_str=None
            )
            logger.debug(f"Done generating tiles for {tile_spec}")

            await self.update_tile_entity_tracker(
                tile_spec=tile_spec, temp_entity_table=entity_table
            )
            logger.debug(f"Done update_tile_entity_tracker for {tile_spec}")

            if progress_callback:
                progress_callback(
                    int(np.floor((index + 1) / num_jobs * TILE_COMPUTE_PROGRESS_MAX_PERCENT)),
                    f"{index+1}/{num_jobs} completed",
                )

    async def tile_job_exists(self, tile_spec: TileSpec) -> bool:
        """
        Get existing tile jobs for the given tile_spec

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec

        Raises
        -------
        TileScheduleNotSupportedError
            if task manager is not initialized

        Returns
        -------
            whether the tile jobs already exist
        """
        if not self._task_manager:
            raise TileScheduleNotSupportedError("Task manager is not initialized")

        scheduler = TileScheduler(task_manager=self._task_manager)

        job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"
        return await scheduler.get_job_details(job_id=job_id) is not None

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
        executor = TileScheduleOnlineStore(
            session=self._session,
            aggregation_id=tile_spec.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
        )
        await executor.execute()

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
            job run details
        """

        if start_ts_str and end_ts_str:
            tile_sql = tile_spec.tile_sql.replace(
                InternalName.TILE_START_DATE_SQL_PLACEHOLDER, f"'{start_ts_str}'"
            ).replace(InternalName.TILE_END_DATE_SQL_PLACEHOLDER, f"'{end_ts_str}'")
        else:
            tile_sql = tile_spec.tile_sql

        tile_generate_ins = TileGenerate(
            session=self._session,
            tile_id=tile_spec.tile_id,
            tile_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
            blind_spot_second=tile_spec.blind_spot_second,
            frequency_minute=tile_spec.frequency_minute,
            sql=tile_sql,
            entity_column_names=tile_spec.entity_column_names,
            value_column_names=tile_spec.value_column_names,
            value_column_types=tile_spec.value_column_types,
            tile_type=tile_type,
            tile_start_date_column=InternalName.TILE_START_DATE,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
            last_tile_start_str=last_tile_start_ts_str,
            aggregation_id=tile_spec.aggregation_id,
        )
        await tile_generate_ins.execute()

        return tile_generate_ins.json()

    async def update_tile_entity_tracker(self, tile_spec: TileSpec, temp_entity_table: str) -> str:
        """
        Update <tile_id>_entity_tracker table for last_tile_start_date

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        temp_entity_table: str
            temporary entity table to be merged into <tile_id>_entity_tracker

        Returns
        -------
            spark job run detail
        """
        if tile_spec.category_column_name is None:
            entity_column_names = tile_spec.entity_column_names
        else:
            entity_column_names = [
                c for c in tile_spec.entity_column_names if c != tile_spec.category_column_name
            ]

        tile_entity_tracking_ins = TileGenerateEntityTracking(
            session=self._session,
            tile_id=tile_spec.aggregation_id,
            entity_column_names=entity_column_names,
            entity_table=temp_entity_table,
            tile_last_start_date_column=InternalName.TILE_LAST_START_DATE.value,
        )

        await tile_entity_tracking_ins.execute()

        return tile_entity_tracking_ins.json()

    async def schedule_online_tiles(
        self,
        tile_spec: TileSpec,
        monitor_periods: int = 10,
    ) -> Optional[str]:
        """
        Schedule online tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        monitor_periods: int
            number of tile periods to monitor and re-generate. Default is 10

        Returns
        -------
            generated sql to be executed or None if the tile job already exists
        """
        sql = await self._schedule_tiles_custom(
            tile_spec=tile_spec,
            tile_type=TileType.ONLINE,
            monitor_periods=monitor_periods,
        )

        return sql

    async def schedule_offline_tiles(
        self,
        tile_spec: TileSpec,
        offline_minutes: int = 1440,
    ) -> Optional[str]:
        """
        Schedule offline tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        offline_minutes: int
            offline tile lookback minutes to monitor and re-generate. Default is 1440

        Returns
        -------
            generated sql to be executed or None if the tile job already exists
        """

        sql = await self._schedule_tiles_custom(
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            offline_minutes=offline_minutes,
        )

        return sql

    async def _schedule_tiles_custom(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
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
        offline_minutes: int
            offline tile lookback minutes
        monitor_periods: int
            online tile lookback period

        Raises
        -------
        TileScheduleNotSupportedError
            if catalog_id or feature_store_id is not provided or task manager is not initialized

        Returns
        -------
            generated sql to be executed or None if the tile job already exists
        """

        logger.info(f"Scheduling {tile_type} tile job for {tile_spec.aggregation_id}")
        job_id = f"{tile_type}_{tile_spec.aggregation_id}"

        if not tile_spec.catalog_id or not tile_spec.feature_store_id:
            raise TileScheduleNotSupportedError("catalog_id and feature_store_id must be provided")

        if not self._task_manager:
            raise TileScheduleNotSupportedError("Task manager is not initialized")

        scheduler = TileScheduler(task_manager=self._task_manager)
        exist_job = await scheduler.get_job_details(job_id=job_id)
        if not exist_job:
            logger.info(f"Creating new job {job_id}")
            tile_schedule_ins = TileGenerateSchedule(
                session=self._session,
                tile_id=tile_spec.tile_id,
                tile_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
                blind_spot_second=tile_spec.blind_spot_second,
                frequency_minute=tile_spec.frequency_minute,
                sql=tile_spec.tile_sql,
                entity_column_names=tile_spec.entity_column_names,
                value_column_names=tile_spec.value_column_names,
                value_column_types=tile_spec.value_column_types,
                tile_type=tile_type,
                offline_period_minute=offline_minutes,
                tile_last_start_date_column=InternalName.TILE_LAST_START_DATE,
                tile_start_date_column=InternalName.TILE_START_DATE,
                tile_start_date_placeholder=InternalName.TILE_START_DATE_SQL_PLACEHOLDER,
                tile_end_date_placeholder=InternalName.TILE_END_DATE_SQL_PLACEHOLDER,
                monitor_periods=monitor_periods,
                aggregation_id=tile_spec.aggregation_id,
            )

            interval_seconds = (
                tile_spec.frequency_minute * 60
                if tile_type == TileType.ONLINE
                else offline_minutes * 60
            )
            await scheduler.start_job_with_interval(
                job_id=job_id,
                interval_seconds=interval_seconds,
                time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
                instance=tile_schedule_ins,
                user_id=tile_spec.user_id,
                feature_store_id=tile_spec.feature_store_id,
                catalog_id=tile_spec.catalog_id,
            )

            return tile_schedule_ins.json()

        return None

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

        Raises
        -------
        TileScheduleNotSupportedError
            if task manager is not initialized
        """
        if not self._task_manager:
            raise TileScheduleNotSupportedError("Task manager is not initialized")

        scheduler = TileScheduler(task_manager=self._task_manager)

        exist_mapping = await self._session.execute_query(
            f"SELECT * FROM TILE_FEATURE_MAPPING WHERE AGGREGATION_ID = '{tile_spec.aggregation_id}' and IS_DELETED = FALSE"
        )
        # only disable tile jobs when there is no tile-feature mapping records for the particular tile
        if exist_mapping is None or len(exist_mapping) == 0:
            logger.info("Stopping job with custom scheduler")
            for t_type in [TileType.ONLINE, TileType.OFFLINE]:
                await scheduler.stop_job(job_id=f"{t_type}_{tile_spec.aggregation_id}")

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
