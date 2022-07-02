"""
Snowflake Tile class
"""
from __future__ import annotations

from typing import Any

from jinja2 import Template
from pydantic import PrivateAttr

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.logger import logger
from featurebyte.models.database_source import DatabaseSourceModel
from featurebyte.models.event_data import TileType
from featurebyte.session.base import BaseSession
from featurebyte.tile.base import TileBase

tm_ins_tile_registry = Template(
    """
    INSERT INTO TILE_REGISTRY (TILE_ID, TILE_SQL) VALUES ('{{tile_id}}', '{{tile_sql}}')
"""
)

tm_gen_tile = Template(
    """
    call SP_TILE_GENERATE(
        '{{sql}}', {{time_modulo_frequency_seconds}}, {{blind_spot_seconds}}, {{frequency_minute}}, '{{column_names}}',
        '{{table_name}}', '{{tile_type.value}}'
    )
"""
)

tm_schedule_tile = Template(
    """
    CREATE OR REPLACE TASK {{temp_task_name}}
      WAREHOUSE = {{warehouse}}
      SCHEDULE = 'USING CRON {{cron}} UTC'
    AS
        call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
            '{{temp_task_name}}', '{{warehouse}}', '{{tile_id}}', {{time_modulo_frequency_seconds}}, {{blind_spot_seconds}},
            {{frequency_minute}}, {{offline_minutes}}, '{{sql}}', '{{column_names}}', '{{tile_type.value}}', {{monitor_periods}}
        )
"""
)


class TileSnowflake(TileBase):
    """
    Snowflake Tile class

    Parameters
    ----------
    tabular_source: DatabaseSourceModel
        snowflake datasource instance
    credentials: Credentials
        credentials to the snowflake datasource
    """

    tabular_source: DatabaseSourceModel
    credentials: Credentials
    _session: BaseSession = PrivateAttr()

    def __init__(self, **kw: Any) -> None:
        """
        Custom constructor for TileSnowflake to instantiate a datasource session with credentials

        Parameters
        ----------
        kw: Any
            constructor arguments
        """
        super().__init__(**kw)
        data_source = ExtendedDatabaseSourceModel(**self.tabular_source.dict())
        self._session = data_source.get_session(credentials=self.credentials)

    def insert_tile_registry(self) -> bool:
        """
        Insert new tile registry record if it does not exist

        Parameters
        ----------

        Returns
        -------
            whether the tile registry record is inserted successfully or not
        """
        result = self._session.execute_query(
            f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{self.tile_id}'"  # nosec
        )
        if result is None or len(result) == 0:
            sql = tm_ins_tile_registry.render(tile_id=self.tile_id, tile_sql=self.tile_sql)
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
            f"UPDATE TILE_REGISTRY SET ENABLED = 'N' WHERE TILE_ID = '{self.tile_id}'"  # nosec
        )

    def generate_tiles(
        self,
        tile_type: TileType,
        start_ts_str: str,
        end_ts_str: str,
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

        Returns
        -------
            tile generation sql
        """
        tile_sql = self.tile_sql.replace("FB_START_TS", f"\\'{start_ts_str}\\'").replace(
            "FB_END_TS", f"\\'{end_ts_str}\\'"
        )
        logger.info(f"tile_sql: {tile_sql}")

        sql = tm_gen_tile.render(
            sql=tile_sql,
            time_modulo_frequency_seconds=self.time_modulo_frequency_seconds,
            blind_spot_seconds=self.blind_spot_seconds,
            frequency_minute=self.frequency_minute,
            column_names=self.column_names,
            table_name=self.tile_id,
            tile_type=tile_type,
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
