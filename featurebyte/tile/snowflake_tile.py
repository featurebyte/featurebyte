"""
Snowflake Tile class
"""
from __future__ import annotations

from jinja2 import Template
from pydantic import BaseModel, Field, root_validator, validator

from featurebyte.config import Credentials
from featurebyte.core.generic import ExtendedDatabaseSourceModel
from featurebyte.logger import logger
from featurebyte.models.event_data import DatabaseSourceModel

tm_gen_tile = Template(
    """
    call SP_TILE_GENERATE(
        '{{sql}}', {{time_modulo_frequency_seconds}}, {{blind_spot_seconds}}, {{frequency_minute}}, '{{column_names}}',
        '{{table_name}}', '{{tile_type}}'
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


class TileSnowflake(BaseModel):
    """
    Snowflake Tile class

    Parameters
    ----------
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

    feature_name: str
    time_modulo_frequency_seconds: int = Field(gt=0)
    blind_spot_seconds: int
    frequency_minute: int = Field(gt=0, le=60)
    tile_sql: str
    column_names: str
    tile_id: str
    tabular_source: DatabaseSourceModel

    @validator("feature_name", "tile_id", "column_names")
    def stripped_upper(cls, value):
        if value is None or value.strip() == "":
            raise ValueError("value cannot be empty")
        return value.strip().upper()

    @root_validator
    def check_time_modulo_frequency_seconds(cls, values):
        if values["time_modulo_frequency_seconds"] > values["frequency_minute"] * 60:
            raise ValueError(
                f"time_modulo_frequency_seconds must be less than {values['frequency_minute'] * 60}"
            )
        return values

    def generate_tiles(
        self,
        tile_type: str,
        start_ts_str: str,
        end_ts_str: str,
        credentials: Credentials | None = None,
    ) -> str:
        """
        Manually trigger tile generation

        Parameters
        ----------
        tile_type: str
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
        self._get_session(credentials).execute_query(sql)

        return sql

    def schedule_online_tiles(
        self,
        monitor_periods: int = 10,
        start_task: bool = True,
        credentials: Credentials | None = None,
    ) -> str:
        """
        Schedule online tiles

        Parameters
        ----------
        monitor_periods: int
            number of tile periods to monitor and re-generate. Default is 10
        start_task: bool
            whether to start the scheduled task

        Returns
        -------
            generated sql to be executed
        """
        tile_type = "ONLINE"
        start_minute = self.time_modulo_frequency_seconds // 60
        cron = f"{start_minute}-59/{self.frequency_minute} * * * *"

        return self._schedule_tiles(
            tile_type=tile_type,
            cron_expr=cron,
            start_task=start_task,
            monitor_periods=monitor_periods,
            credentials=credentials,
        )

    def schedule_offline_tiles(
        self,
        offline_minutes: int = 1440,
        start_task: bool = True,
        credentials: Credentials | None = None,
    ) -> str:
        """
        Schedule offline tiles

        Parameters
        ----------
        offline_minutes: int
            offline tile lookback minutes to monitor and re-generate. Default is 1440
        start_task: bool
            whether to start the scheduled task

        Returns
        -------
            generated sql to be executed
        """
        tile_type = "OFFLINE"
        start_minute = self.time_modulo_frequency_seconds // 60
        cron = f"{start_minute} 0 * * *"

        return self._schedule_tiles(
            tile_type=tile_type,
            cron_expr=cron,
            start_task=start_task,
            offline_minutes=offline_minutes,
            credentials=credentials,
        )

    def _schedule_tiles(
        self,
        tile_type: str,
        cron_expr: str,
        start_task: bool,
        offline_minutes: int = 1440,
        monitor_periods: int = 10,
        credentials: Credentials | None = None,
    ) -> str:
        """
        Common tile schedule method

        Parameters
        ----------
        start_task: bool
            whether to start the scheduled task
        tile_type: str
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
        session = self._get_session(credentials)

        sql = tm_schedule_tile.render(
            temp_task_name=temp_task_name,
            warehouse=session.warehouse,
            cron=cron_expr,
            sql=self.tile_sql,
            time_modulo_frequency_seconds=self.time_modulo_frequency_seconds,
            blind_spot_seconds=self.blind_spot_seconds,
            frequency_minute=self.frequency_minute,
            column_names=self.column_names,
            tile_id=self.tile_id,
            type=tile_type,
            offline_minutes=offline_minutes,
            monitor_periods=monitor_periods,
        )

        logger.info(f"generated sql: {sql}")
        session.execute_query(sql)

        if start_task:
            session.execute_query(f"ALTER TASK {temp_task_name} RESUME")

        return sql

    def _get_session(self, credentials: Credentials | None = None):
        data_source = ExtendedDatabaseSourceModel(**self.tabular_source.dict())
        session = data_source.get_session(credentials=credentials)
        return session
