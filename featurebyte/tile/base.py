"""
Base Tile class
"""
from __future__ import annotations

from typing import Any, List, Optional, Tuple

from abc import ABC, abstractmethod
from datetime import datetime

from pydantic import BaseModel, PrivateAttr

from featurebyte.common import date_util
from featurebyte.enum import InternalName
from featurebyte.exception import TileScheduleNotSupportedError
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.sql.spark.tile_generate_schedule import TileGenerateSchedule
from featurebyte.tile.scheduler import TileScheduler


class BaseTileManager(BaseModel, ABC):
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

    async def generate_tiles_on_demand(self, tile_inputs: List[Tuple[TileSpec, str]]) -> None:
        """
        Generate Tiles and update tile entity checking table

        Parameters
        ----------
        tile_inputs: List[Tuple[TileSpec, str]]
            list of TileSpec, temp_entity_table to update the feature store
        """
        for tile_spec, entity_table in tile_inputs:

            await self.generate_tiles(
                tile_spec=tile_spec, tile_type=TileType.OFFLINE, start_ts_str=None, end_ts_str=None
            )
            logger.debug(f"Done generating tiles for {tile_spec}")

            await self.update_tile_entity_tracker(
                tile_spec=tile_spec, temp_entity_table=entity_table
            )
            logger.debug(f"Done update_tile_entity_tracker for {tile_spec}")

    @abstractmethod
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

    @abstractmethod
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

    @abstractmethod
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
        """

    @abstractmethod
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
            generated sql to be executed
        """

    async def schedule_online_tiles(
        self,
        tile_spec: TileSpec,
        monitor_periods: int = 10,
        schedule_time: datetime = datetime.utcnow(),
    ) -> str:
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

        sql = await self._schedule_tiles_custom(
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
    ) -> str:
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

        sql = await self._schedule_tiles_custom(
            tile_spec=tile_spec,
            tile_type=TileType.ONLINE,
            next_job_time=next_job_time,
            offline_minutes=offline_minutes,
        )

        return sql

    async def _schedule_tiles_custom(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        next_job_time: datetime,
        offline_minutes: int = 1440,
        monitor_periods: int = 10,
    ) -> str:
        """
        Common tile schedule method

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        tile_type: TileType
            ONLINE or OFFLINE
        next_job_time: datetime
            next tile job start time
        offline_minutes: int
            offline tile lookback minutes
        monitor_periods: int
            online tile lookback period

        Raises
        -------
        TileScheduleNotSupportedError
            if user_id, workspace_id or feature_store_id is not provided or task manager is not initialized

        Returns
        -------
            generated sql to be executed
        """

        logger.info(f"Scheduling {tile_type} tile job for {tile_spec.aggregation_id}")
        job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

        if not tile_spec.user_id or not tile_spec.workspace_id or not tile_spec.feature_store_id:
            raise TileScheduleNotSupportedError(
                "user_id, workspace_id and feature_store_id must be provided"
            )

        if not self._task_manager:
            raise TileScheduleNotSupportedError("Task manager is not initialized")

        tile_schedule_ins = TileGenerateSchedule(
            spark_session=self._session,
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
            agg_id=tile_spec.aggregation_id,
            job_schedule_ts=next_job_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        )

        scheduler = TileScheduler(task_manager=self._task_manager)

        await scheduler.start_job_with_interval(
            job_id=job_id,
            interval_seconds=tile_spec.frequency_minute * 60,
            start_after=next_job_time,
            instance=tile_schedule_ins,
            user_id=tile_spec.user_id,
            feature_store_id=tile_spec.feature_store_id,
            workspace_id=tile_spec.workspace_id,
        )

        return tile_schedule_ins.json()

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
            if user_id, workspace_id or feature_store_id is not provided or task manager is not initialized
        """
        if not tile_spec.user_id or not tile_spec.workspace_id or not tile_spec.feature_store_id:
            raise TileScheduleNotSupportedError(
                "user_id, workspace_id and feature_store_id must be provided"
            )

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
