"""
Snowflake Tile class
"""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, PrivateAttr

from featurebyte.enum import InternalName
from featurebyte.logger import logger
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.query_graph.sql import escape_column_names
from featurebyte.session.base import BaseSession
from featurebyte.tile.snowflake_sql_template import (
    tm_generate_tile,
    tm_insert_tile_registry,
    tm_schedule_tile,
    tm_select_tile_registry,
    tm_tile_entity_tracking,
    tm_update_tile_registry,
)


class TileManagerSnowflake(BaseModel):
    """
    Snowflake Tile class
    """

    _session: BaseSession = PrivateAttr()

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

    def insert_tile_registry(self, tile_spec: TileSpec) -> bool:
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
        result = self._session.execute_query(
            tm_select_tile_registry.render(tile_id=tile_spec.tile_id)
        )
        if result is None or len(result) == 0:
            sql = tm_insert_tile_registry.render(
                tile_id=tile_spec.tile_id,
                tile_sql=tile_spec.tile_sql,
                entity_column_names=",".join(escape_column_names(tile_spec.entity_column_names)),
                value_column_names=",".join(tile_spec.value_column_names),
                time_modulo_frequency_second=tile_spec.time_modulo_frequency_second,
                blind_spot_second=tile_spec.blind_spot_second,
                frequency_minute=tile_spec.frequency_minute,
            )
            logger.debug(f"generated tile insert sql: {sql}")
            self._session.execute_query(sql)
            return True

        logger.warning(f"Tile id {tile_spec.tile_id} already exists")
        return False

    def disable_tiles(self, tile_spec: TileSpec) -> None:
        """
        Disable tile jobs

        Parameters
        ----------
        tile_spec: TileSpec
            the input TileSpec
        """
        for t_type in TileType:
            tile_task_name = f"TILE_TASK_{t_type}_{tile_spec.tile_id}"
            self._session.execute_query(f"ALTER TASK IF EXISTS {tile_task_name} SUSPEND")

        self._session.execute_query(
            tm_update_tile_registry.render(tile_id=tile_spec.tile_id, is_enabled=False)
        )

    def update_tile_entity_tracker(self, tile_spec: TileSpec, temp_entity_table: str) -> str:
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
        logger.debug(f"generated sql: {sql}")
        self._session.execute_query(sql)

        return sql

    def generate_tiles(
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
        logger.debug(f"tile_sql: {tile_sql}")

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
            tile_id=tile_spec.tile_id,
            tile_type=tile_type,
            last_tile_start_ts_str=last_tile_start_ts_str,
        )
        logger.debug(f"generated sql: {sql}")
        self._session.execute_query(sql)

        return sql

    def schedule_online_tiles(
        self,
        tile_spec: TileSpec,
        monitor_periods: int = 10,
    ) -> str:
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
            generated sql to be executed
        """
        start_minute = tile_spec.time_modulo_frequency_second // 60
        cron = f"{start_minute}-59/{tile_spec.frequency_minute} * * * *"

        return self._schedule_tiles(
            tile_spec=tile_spec,
            tile_type=TileType.ONLINE,
            cron_expr=cron,
            monitor_periods=monitor_periods,
        )

    def schedule_offline_tiles(
        self,
        tile_spec: TileSpec,
        offline_minutes: int = 1440,
    ) -> str:
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
            generated sql to be executed
        """
        start_minute = tile_spec.time_modulo_frequency_second // 60
        cron = f"{start_minute} 0 * * *"

        return self._schedule_tiles(
            tile_spec=tile_spec,
            tile_type=TileType.OFFLINE,
            cron_expr=cron,
            offline_minutes=offline_minutes,
        )

    def _schedule_tiles(
        self,
        tile_spec: TileSpec,
        tile_type: TileType,
        cron_expr: str,
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

        temp_task_name = f"SHELL_TASK_{tile_spec.tile_id}_{tile_type}"

        sql = tm_schedule_tile.render(
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
            tile_id=tile_spec.tile_id,
            tile_type=tile_type,
            offline_minutes=offline_minutes,
            monitor_periods=monitor_periods,
        )

        logger.debug(f"generated sql: {sql}")
        self._session.execute_query(sql)

        self._session.execute_query(f"ALTER TASK IF EXISTS {temp_task_name} RESUME")

        return sql
