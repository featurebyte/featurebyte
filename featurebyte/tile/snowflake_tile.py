"""
Snowflake Tile class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import PrivateAttr

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.logger import logger
from featurebyte.models.feature import TileType
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.session.base import BaseSession
from featurebyte.tile.base import TileBase
from featurebyte.tile.snowflake_sql_template import (
    tm_generate_tile,
    tm_insert_tile_registry,
    tm_schedule_tile,
    tm_select_tile_registry,
    tm_update_tile_registry,
)


class TileSnowflake(TileBase):
    """
    Snowflake Tile class
    """

    _session: BaseSession = PrivateAttr()

    def __init__(self, session: BaseSession, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session

        Parameters
        ----------
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        self._session = session

    def insert_tile_registry(self) -> bool:
        """
        Insert new tile registry record if it does not exist

        Parameters
        ----------

        Returns
        -------
            whether the tile registry record is inserted successfully or not
        """
        result = self._session.execute_query(tm_select_tile_registry.render(tile_id=self.tile_id))
        if result is None or len(result) == 0:
            sql = tm_insert_tile_registry.render(
                tile_id=self.tile_id,
                tile_sql=self.tile_sql,
                column_names=self.column_names,
                time_modulo_frequency_seconds=self.time_modulo_frequency_seconds,
                blind_spot_seconds=self.blind_spot_seconds,
                frequency_minute=self.frequency_minute,
            )
            logger.info(f"generated tile insert sql: {sql}")
            self._session.execute_query(sql)
            return True

        logger.warning(f"Tile id {self.tile_id} already exists")
        return False

    def disable_tiles(self) -> None:
        """
        Disable tile jobs

        Parameters
        ----------

        """
        for t_type in TileType:
            tile_task_name = f"TILE_TASK_{t_type}_{self.tile_id}"
            self._session.execute_query(f"ALTER TASK IF EXISTS {tile_task_name} SUSPEND")

        self._session.execute_query(
            tm_update_tile_registry.render(tile_id=self.tile_id, is_enabled=False)
        )

    def generate_tiles(
        self,
        tile_type: TileType,
        start_ts_str: str,
        end_ts_str: str,
        last_tile_start_ts_str: Optional[str] = None,
    ) -> str:
        """
        Manually trigger tile generation

        Parameters
        ----------
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
        tile_sql = self.tile_sql.replace("FB_START_TS", f"\\'{start_ts_str}\\'").replace(
            "FB_END_TS", f"\\'{end_ts_str}\\'"
        )
        logger.info(f"tile_sql: {tile_sql}")

        if last_tile_start_ts_str:
            last_tile_start_ts_str = f"'{last_tile_start_ts_str}'"
        else:
            last_tile_start_ts_str = "null"

        sql = tm_generate_tile.render(
            tile_sql=tile_sql,
            time_modulo_frequency_seconds=self.time_modulo_frequency_seconds,
            blind_spot_seconds=self.blind_spot_seconds,
            frequency_minute=self.frequency_minute,
            column_names=self.column_names,
            tile_id=self.tile_id,
            tile_type=tile_type,
            last_tile_start_ts_str=last_tile_start_ts_str,
        )
        logger.info(f"generated sql: {sql}")
        self._session.execute_query(sql)

        return sql

    def schedule_online_tiles(
        self,
        monitor_periods: int = 10,
    ) -> str:
        """
        Schedule online tiles

        Parameters
        ----------
        monitor_periods: int
            number of tile periods to monitor and re-generate. Default is 10

        Returns
        -------
            generated sql to be executed
        """
        start_minute = self.time_modulo_frequency_seconds // 60
        cron = f"{start_minute}-59/{self.frequency_minute} * * * *"

        return self._schedule_tiles(
            tile_type=TileType.ONLINE,
            cron_expr=cron,
            monitor_periods=monitor_periods,
        )

    def schedule_offline_tiles(
        self,
        offline_minutes: int = 1440,
    ) -> str:
        """
        Schedule offline tiles

        Parameters
        ----------
        offline_minutes: int
            offline tile lookback minutes to monitor and re-generate. Default is 1440

        Returns
        -------
            generated sql to be executed
        """
        start_minute = self.time_modulo_frequency_seconds // 60
        cron = f"{start_minute} 0 * * *"

        return self._schedule_tiles(
            tile_type=TileType.OFFLINE,
            cron_expr=cron,
            offline_minutes=offline_minutes,
        )

    def _schedule_tiles(
        self,
        tile_type: TileType,
        cron_expr: str,
        offline_minutes: int = 1440,
        monitor_periods: int = 10,
    ) -> str:
        """
        Common tile schedule method

        Parameters
        ----------
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

        temp_task_name = f"SHELL_TASK_{self.tile_id}_{tile_type}"

        sql = tm_schedule_tile.render(
            temp_task_name=temp_task_name,
            warehouse=self._session.dict()["warehouse"],
            cron=cron_expr,
            sql=self.tile_sql,
            time_modulo_frequency_seconds=self.time_modulo_frequency_seconds,
            blind_spot_seconds=self.blind_spot_seconds,
            frequency_minute=self.frequency_minute,
            column_names=self.column_names,
            tile_id=self.tile_id,
            tile_type=tile_type,
            offline_minutes=offline_minutes,
            monitor_periods=monitor_periods,
        )

        logger.info(f"generated sql: {sql}")
        self._session.execute_query(sql)

        self._session.execute_query(f"ALTER TASK IF EXISTS {temp_task_name} RESUME")

        return sql
