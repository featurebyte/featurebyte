"""
Snowflake Tile class
"""
from __future__ import annotations

from datetime import datetime

from jinja2 import Template
from logzero import logger

from ..session.snowflake import SnowflakeSession
from .base import TileBase

tm_gen_tile = Template(
    """
    call SP_TILE_GENERATE(
        '{{sql}}', {{time_modulo_frequency_seconds}}, {{blind_spot_seconds}}, {{frequency_minute}}, '{{column_names}}',
        '{{table_name}}'
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
            {{frequency_minute}}, {{offline_minutes}}, '{{sql}}', '{{column_names}}', '{{type}}', {{monitor_periods}}
        )
"""
)


class TileSnowflake(TileBase):
    """
    Snowflake Tile class
    """

    def __init__(
        self,
        session: SnowflakeSession,
        feature_name: str,
        time_modulo_frequency_seconds: int,
        blind_spot_seconds: int,
        frequency_minute: int,
        tile_sql: str,
        column_names: str,
        tile_id: str,
    ) -> None:
        """
        Instantiate a TileSnowflake instance.

        Parameters
        ----------
        session: SnowflakeSession
            snowflake session instance
        feature_name: str
            feature name
        time_modulo_frequency_seconds: int
            time modulo seconds for the tile
        blind_spot_seconds: int
            blind spot seconds for the tile
        frequency_minute: int
            frequency minute for the tile
        tile_sql: str
            sql for tile generation
        column_names: str
            comma separated string of column names for the tile table
        tile_id: str
            hash value of tile id and name
        """

        self.validate(
            feature_name,
            time_modulo_frequency_seconds,
            blind_spot_seconds,
            frequency_minute,
            tile_sql,
            column_names,
            tile_id,
        )

        self._session = session
        self._feature_name = feature_name.strip().upper()
        self._time_modulo_frequency_seconds = time_modulo_frequency_seconds
        self._blind_spot_seconds = blind_spot_seconds
        self._frequency_minute = frequency_minute
        self._tile_sql = tile_sql
        self._column_names = column_names
        self._tile_id = tile_id

    def generate_tiles(self, start_ts_str: str, end_ts_str: str) -> str:
        """
        Manually trigger tile generation

        Parameters
        ----------
        start_ts_str: str
            start_timestamp of tile. ie. 2022-06-20 15:00:00
        end_ts_str: str
            end_timestamp of tile. ie. 2022-06-21 15:00:00

        Returns
        -------
        sql: str
            tile generation sql
        """
        tile_sql = self._tile_sql.replace("FB_START_TS", f"\\'{start_ts_str}\\'").replace(
            "FB_END_TS", f"\\'{end_ts_str}\\'"
        )
        logger.info(f"tile_sql: {tile_sql}")

        sql = tm_gen_tile.render(
            sql=tile_sql,
            time_modulo_frequency_seconds=self._time_modulo_frequency_seconds,
            blind_spot_seconds=self._blind_spot_seconds,
            frequency_minute=self._frequency_minute,
            column_names=self._column_names,
            table_name=self._tile_id,
        )
        logger.info(f"generated sql: {sql}")
        self._session.execute_query(sql)

        return sql

    # def schedule_tiles(
    #     self,
    #     tile_type: str,
    #     offline_frequency: Optional[int] = None,
    #     monitor_periods: Optional[int] = None,
    #     end_ts: Optional[str] = None,
    # ) -> None:
    #
    #     tile_type = tile_type.upper()
    #     if tile_type not in ["ONLINE", "OFFLINE"]:
    #         raise ValueError("tile type must be either ONLINE or OFFLINE")
    #
    #     if tile_type == "OFFLINE":
    #         if not offline_frequency:
    #             raise ValueError("offline_frequency must not be empty")
    #         else:
    #             self._check_integer_range(offline_frequency, self._frequency)
    #
    #     sql, task_name = self._construct_schedule_tile_generate_sql(
    #         tile_type, offline_frequency, monitor_periods, end_ts
    #     )
    #     logger.info(sql)
    #     self._execute(sql)
    #
    #     if not end_ts:
    #         logger.info(f"task_name: {task_name}")
    #         self._execute(f"ALTER TASK {task_name} RESUME")

    # def _construct_tile_generate_sql(self, table_name: str, start_ts: str, end_ts: str) -> str:
    #     sql = (
    #         sql_generate_tile.replace("${SQL}", f"'{self._tile_sql}'")
    #         .replace("FB_START_TS", f"\\'{start_ts}\\'")
    #         .replace("FB_END_TS", f"\\'{end_ts}\\'")
    #         .replace("${COLUMN_NAMES}", f"'{','.join(self._column_names)}'")
    #         .replace("${TABLE_NAME}", f"'{table_name}'")
    #         .replace("${WINDOW_END_MINUTE}", f"{self._window_end_minute}")
    #         .replace("${FREQUENCY_MINUTE}", f"{self._frequency}")
    #     )
    #
    #     return sql

    # def _construct_schedule_tile_generate_sql(
    #     self, tile_type: str, offline_frequency: int, monitor_periods: int, end_ts: int
    # ) -> Tuple[str, str]:
    #
    #     blindspot_residue_seconds = self._blind_spot_seconds % 60
    #     target_sql_template = (
    #         sql_manual_generate_monitor_title if end_ts else sql_schedule_generate_monitor_title
    #     )
    #     task_name = f"TASK_{self._feature_name}_{tile_type}_TILE"
    #
    #     if not monitor_periods:
    #         monitor_periods = 2
    #
    #     frequency = self._frequency
    #     if tile_type == "OFFLINE":
    #         if not offline_frequency:
    #             frequency = 1440
    #         else:
    #             frequency = offline_frequency
    #
    #     sql = (
    #         target_sql_template.replace("${FEATURE_NAME}", f"'{self._feature_name}'")
    #         .replace("${SQL}", f"'{self._tile_sql}'")
    #         .replace("${COLUMN_NAMES}", f"'{','.join(self._column_names)}'")
    #         .replace("${TYPE}", f"'{tile_type}'")
    #         .replace("${WINDOW_END_MINUTE}", f"{self._window_end_minute}")
    #         .replace("${FREQUENCY_MINUTE}", f"{frequency}")
    #         .replace("${BLINDSPOT_RESIDUE_SECONDS}", f"{blindspot_residue_seconds}")
    #         .replace("${MONITOR_PERIODS}", f"{monitor_periods}")
    #     )
    #
    #     if end_ts:
    #         sql = sql.replace("${END_TS}", f"'{end_ts}'")
    #     else:
    #         window_modular_minute = self._window_end_minute + self._blind_spot_seconds // 60
    #         cron_expression = f"{window_modular_minute}-59/{self._frequency} * * * *"
    #
    #         sql = (
    #             sql.replace("${END_TS}", f"NULL")
    #             .replace("${task_name}", f"{task_name}")
    #             .replace("${warehouse}", f"{self._conn.warehouse}")
    #             .replace("${cron}", f"{cron_expression}")
    #         )
    #
    #     return sql, task_name
