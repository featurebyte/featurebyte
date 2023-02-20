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
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.base import BaseSession
from featurebyte.tile.scheduler import TileScheduler
from featurebyte.tile.sql_template import tm_schedule_tile
from featurebyte.utils.snowflake.sql import escape_column_names


class BaseTileManager(BaseModel, ABC):
    """
    Base Tile class
    """

    _session: BaseSession = PrivateAttr()
    _scheduler: TileScheduler = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        session: BaseSession
            input session for datasource
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session
        self._scheduler = TileScheduler()

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

        Returns
        -------
            generated sql to be executed
        """

        logger.info(f"Scheduling {tile_type} tile job for {tile_spec.aggregation_id}")
        job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

        sql = tm_schedule_tile.render(
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
            tile_type=TileType.ONLINE,
            offline_minutes=offline_minutes,
            monitor_periods=monitor_periods,
        )

        self._scheduler.start_job_with_interval(
            job_id=job_id,
            interval_seconds=tile_spec.frequency_minute * 60,
            start_from=next_job_time,
            func=self._session.execute_query,
            args=[sql],
        )

        return sql

    @abstractmethod
    async def remove_tile_jobs(
        self,
        tile_spec: TileSpec,
    ) -> None:
        """
        Schedule offline tiles

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        """
