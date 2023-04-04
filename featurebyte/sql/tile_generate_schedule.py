"""
Tile Generate Schedule script for SP_TILE_GENERATE
"""
from typing import Any, Dict, List

from datetime import datetime, timedelta

import dateutil.parser
from pydantic import Field

from featurebyte.enum import InternalName
from featurebyte.logger import logger
from featurebyte.sql.common import retry_sql
from featurebyte.sql.tile_common import TileCommon
from featurebyte.sql.tile_generate import TileGenerate
from featurebyte.sql.tile_monitor import TileMonitor
from featurebyte.sql.tile_schedule_online_store import TileScheduleOnlineStore


class TileGenerateSchedule(TileCommon):
    """
    Tile Generate Schedule script corresponding to SP_TILE_GENERATE_SCHEDULE stored procedure
    """

    offline_period_minute: int
    tile_start_date_column: str
    tile_last_start_date_column: str
    tile_start_date_placeholder: str
    tile_end_date_placeholder: str
    tile_type: str
    monitor_periods: int
    job_schedule_ts: str = Field(default=datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"))

    # pylint: disable=too-many-locals,too-many-statements
    async def execute(self) -> None:
        """
        Execute tile generate schedule operation

        Raises
        ------
        Exception
            Related exception from the triggered stored procedures if it fails
        """

        last_tile_end_ts = dateutil.parser.isoparse(self.job_schedule_ts)
        cron_residue_seconds = self.tile_modulo_frequency_second % 60
        last_tile_end_ts = last_tile_end_ts.replace(second=cron_residue_seconds)
        last_tile_end_ts = last_tile_end_ts - timedelta(seconds=self.blind_spot_second)
        date_format = "%Y-%m-%d %H:%M:%S"

        tile_type = self.tile_type.upper()
        lookback_period = self.frequency_minute * (self.monitor_periods + 1)
        tile_id = self.tile_id.upper()

        tile_end_ts = last_tile_end_ts
        if tile_type == "OFFLINE":
            lookback_period = self.offline_period_minute
            tile_end_ts = tile_end_ts - timedelta(minutes=lookback_period)

        tile_start_ts = tile_end_ts - timedelta(minutes=lookback_period)
        tile_start_ts_str = tile_start_ts.strftime(date_format)
        monitor_tile_start_ts_str = tile_start_ts_str

        # use the last_tile_start_date from tile registry as tile_start_ts_str if it is earlier than tile_start_ts_str
        registry_df = await self._session.execute_query(
            f"SELECT LAST_TILE_START_DATE_ONLINE FROM TILE_REGISTRY WHERE TILE_ID = '{self.tile_id}' AND LAST_TILE_START_DATE_ONLINE IS NOT NULL"
        )
        if registry_df is not None and len(registry_df) > 0:
            registry_last_tile_start_ts = registry_df["LAST_TILE_START_DATE_ONLINE"].iloc[0]
            logger.info(f"Last tile start date from registry - {registry_last_tile_start_ts}")

            if registry_last_tile_start_ts.timestamp() < tile_start_ts.timestamp():
                logger.info(
                    f"Use last tile start date from registry - {registry_last_tile_start_ts} instead of {tile_start_ts_str}"
                )
                tile_start_ts_str = registry_last_tile_start_ts.strftime(date_format)

        session_id = f"{tile_id}|{datetime.now()}"
        audit_insert_sql = f"""INSERT INTO TILE_JOB_MONITOR
        (
            TILE_ID,
            AGGREGATION_ID,
            TILE_TYPE,
            SESSION_ID,
            STATUS,
            MESSAGE,
            CREATED_AT
        )
            VALUES
        (
            '{tile_id}',
            '{self.aggregation_id}',
            '{tile_type}',
            '{session_id}',
            '<STATUS>',
            '<MESSAGE>',
            current_timestamp()
        )"""
        logger.debug(audit_insert_sql)

        insert_sql = audit_insert_sql.replace("<STATUS>", "STARTED").replace("<MESSAGE>", "")
        logger.debug(insert_sql)
        await retry_sql(self._session, insert_sql)

        monitor_end_ts = tile_end_ts - timedelta(minutes=self.frequency_minute)
        monitor_tile_end_ts_str = monitor_end_ts.strftime(date_format)

        monitor_input_sql = self.sql.replace(
            f"{self.tile_start_date_placeholder}", "'" + monitor_tile_start_ts_str + "'"
        ).replace(f"{self.tile_end_date_placeholder}", "'" + monitor_tile_end_ts_str + "'")

        tile_end_ts_str = tile_end_ts.strftime(date_format)
        generate_input_sql = self.sql.replace(
            f"{self.tile_start_date_placeholder}", "'" + tile_start_ts_str + "'"
        ).replace(f"{self.tile_end_date_placeholder}", "'" + tile_end_ts_str + "'")

        last_tile_start_ts = tile_end_ts - timedelta(minutes=self.frequency_minute)
        last_tile_start_str = last_tile_start_ts.strftime(date_format)

        tile_monitor_ins = TileMonitor(
            session=self._session,
            tile_id=tile_id,
            tile_modulo_frequency_second=self.tile_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            sql=generate_input_sql,
            monitor_sql=monitor_input_sql,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_type=self.tile_type,
            tile_start_date_column=InternalName.TILE_START_DATE,
            aggregation_id=self.aggregation_id,
        )

        tile_generate_ins = TileGenerate(
            session=self._session,
            tile_id=tile_id,
            tile_modulo_frequency_second=self.tile_modulo_frequency_second,
            blind_spot_second=self.blind_spot_second,
            frequency_minute=self.frequency_minute,
            sql=generate_input_sql,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_type=self.tile_type,
            tile_start_date_column=InternalName.TILE_START_DATE,
            last_tile_start_str=last_tile_start_str,
            tile_last_start_date_column=self.tile_last_start_date_column,
            aggregation_id=self.aggregation_id,
        )

        tile_online_store_ins = TileScheduleOnlineStore(
            session=self._session,
            aggregation_id=self.aggregation_id,
            job_schedule_ts_str=last_tile_end_ts.strftime(date_format),
        )

        step_specs: List[Dict[str, Any]] = [
            {
                "name": "tile_monitor",
                "trigger": tile_monitor_ins,
                "status": {
                    "fail": "MONITORED_FAILED",
                    "success": "MONITORED",
                },
            },
            {
                "name": "tile_generate",
                "trigger": tile_generate_ins,
                "status": {
                    "fail": "GENERATED_FAILED",
                    "success": "GENERATED",
                },
            },
            {
                "name": "tile_online_store",
                "trigger": tile_online_store_ins,
                "status": {
                    "fail": "ONLINE_STORE_FAILED",
                    "success": "COMPLETED",
                },
            },
        ]

        for spec in step_specs:
            try:
                logger.info(f"Calling {spec['name']}\n")
                tile_ins: TileCommon = spec["trigger"]
                await tile_ins.execute()
                logger.info(f"End of calling {spec['name']}\n")
            except Exception as exception:
                message = str(exception).replace("'", "")
                fail_code = spec["status"]["fail"]

                ex_insert_sql = audit_insert_sql.replace("<STATUS>", fail_code).replace(
                    "<MESSAGE>", message
                )
                logger.error("fail_insert_sql: ", ex_insert_sql)
                await retry_sql(self._session, ex_insert_sql)
                raise exception

            success_code = spec["status"]["success"]
            insert_sql = audit_insert_sql.replace("<STATUS>", success_code).replace("<MESSAGE>", "")
            logger.info("success_insert_sql: ", insert_sql)
            await retry_sql(self._session, insert_sql)
