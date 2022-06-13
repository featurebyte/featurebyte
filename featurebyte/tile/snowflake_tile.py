from typing import Any, Optional, Tuple

import math
import numbers

from logzero import logger

sql_generate_tile = """
    call SP_TILE_GENERATE(
        ${SQL}, ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE}, ${COLUMN_NAMES}, ${TABLE_NAME}
    )
"""

sql_manual_generate_monitor_title = """
    call SP_TILE_GENERATE_SCHEDULE(
        ${FEATURE_NAME}, ${WINDOW_END_MINUTE}, ${BLINDSPOT_RESIDUE_SECONDS}, ${FREQUENCY_MINUTE}, ${SQL},
        ${COLUMN_NAMES}, ${TYPE}, ${MONITOR_PERIODS}, ${END_TS}
    )
"""

sql_schedule_generate_monitor_title = """
    CREATE OR REPLACE TASK ${task_name}
      WAREHOUSE = ${warehouse}
      SCHEDULE = 'USING CRON ${cron} UTC'
    AS
        call SP_TILE_GENERATE_SCHEDULE(
            ${FEATURE_NAME}, ${WINDOW_END_MINUTE}, ${BLINDSPOT_RESIDUE_SECONDS}, ${FREQUENCY_MINUTE}, ${SQL},
            ${COLUMN_NAMES}, ${TYPE}, ${MONITOR_PERIODS}, ${END_TS}
        )
"""


class TileSnowflake:
    def __init__(
        self,
        conn,
        feature_name,
        window_end_minute,
        blind_spot_seconds,
        frequency,
        tile_sql,
        column_names,
    ):
        self._conn = conn
        self._check_integer_range(frequency, 1, 60)
        self._check_integer_range(window_end_minute, 0)
        self._check_integer_range(blind_spot_seconds, 0)

        if 60 % frequency != 0:
            raise ValueError("base_window value must be divisible by 60")

        if feature_name is None or feature_name.strip() == "":
            raise ValueError("feature name cannot be empty")

        self._conn = conn
        self._feature_name = feature_name.strip().upper()
        self._window_end_minute = window_end_minute
        self._blind_spot_seconds = blind_spot_seconds
        self._frequency = frequency
        self._tile_sql = tile_sql
        self._column_names = column_names

    def generate_tiles(self, table_name: str, start_ts: str, end_ts: str) -> None:
        sql = self._construct_tile_generate_sql(table_name, start_ts, end_ts)
        logger.info(sql)
        self._execute(sql)

    def schedule_tiles(
        self,
        tile_type: str,
        offline_frequency: Optional[int] = None,
        monitor_periods: Optional[int] = None,
        end_ts: Optional[str] = None,
    ) -> None:

        tile_type = tile_type.upper()
        if tile_type not in ["ONLINE", "OFFLINE"]:
            raise ValueError("tile type must be either ONLINE or OFFLINE")

        if tile_type == "OFFLINE":
            if not offline_frequency:
                raise ValueError("offline_frequency must not be empty")
            else:
                self._check_integer_range(offline_frequency, self._frequency)

        sql, task_name = self._construct_schedule_tile_generate_sql(
            tile_type, offline_frequency, monitor_periods, end_ts
        )
        logger.info(sql)
        self._execute(sql)

        if not end_ts:
            logger.info(f"task_name: {task_name}")
            self._execute(f"ALTER TASK {task_name} RESUME")

    def _construct_tile_generate_sql(self, table_name: str, start_ts: str, end_ts: str) -> str:
        sql = (
            sql_generate_tile.replace("${SQL}", f"'{self._tile_sql}'")
            .replace("FB_START_TS", f"\\'{start_ts}\\'")
            .replace("FB_END_TS", f"\\'{end_ts}\\'")
            .replace("${COLUMN_NAMES}", f"'{','.join(self._column_names)}'")
            .replace("${TABLE_NAME}", f"'{table_name}'")
            .replace("${WINDOW_END_MINUTE}", f"{self._window_end_minute}")
            .replace("${FREQUENCY_MINUTE}", f"{self._frequency}")
        )

        return sql

    def _construct_schedule_tile_generate_sql(
        self, tile_type: str, offline_frequency: int, monitor_periods: int, end_ts: int
    ) -> Tuple[str, str]:

        blindspot_residue_seconds = self._blind_spot_seconds % 60
        target_sql_template = (
            sql_manual_generate_monitor_title if end_ts else sql_schedule_generate_monitor_title
        )
        task_name = f"TASK_{self._feature_name}_{tile_type}_TILE"

        if not monitor_periods:
            monitor_periods = 2

        frequency = self._frequency
        if tile_type == "OFFLINE":
            if not offline_frequency:
                frequency = 1440
            else:
                frequency = offline_frequency

        sql = (
            target_sql_template.replace("${FEATURE_NAME}", f"'{self._feature_name}'")
            .replace("${SQL}", f"'{self._tile_sql}'")
            .replace("${COLUMN_NAMES}", f"'{','.join(self._column_names)}'")
            .replace("${TYPE}", f"'{tile_type}'")
            .replace("${WINDOW_END_MINUTE}", f"{self._window_end_minute}")
            .replace("${FREQUENCY_MINUTE}", f"{frequency}")
            .replace("${BLINDSPOT_RESIDUE_SECONDS}", f"{blindspot_residue_seconds}")
            .replace("${MONITOR_PERIODS}", f"{monitor_periods}")
        )

        if end_ts:
            sql = sql.replace("${END_TS}", f"'{end_ts}'")
        else:
            window_modular_minute = self._window_end_minute + self._blind_spot_seconds // 60
            cron_expression = f"{window_modular_minute}-59/{self._frequency} * * * *"

            sql = (
                sql.replace("${END_TS}", f"NULL")
                .replace("${task_name}", f"{task_name}")
                .replace("${warehouse}", f"{self._conn.warehouse}")
                .replace("${cron}", f"{cron_expression}")
            )

        return sql, task_name

    def _check_integer_range(self, val, lower, upper=math.inf):
        if not isinstance(val, numbers.Integral) or val < lower or upper > upper:
            raise ValueError(f"{val} must be an integer between {lower} and {upper}")

    def _execute(self, sql: str) -> Any:
        cs = self._conn.cursor()
        try:
            cs.execute(sql)
            return cs.fetchall()
        finally:
            cs.close()
