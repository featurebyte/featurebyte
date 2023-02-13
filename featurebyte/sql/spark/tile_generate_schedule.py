"""
Tile Generate Schedule script for SP_TILE_GENERATE
"""
from typing import Optional

from datetime import datetime, timedelta

import dateutil.parser

from featurebyte.enum import InternalName
from featurebyte.logger import logger
from featurebyte.sql.spark.tile_common import TileCommon
from featurebyte.sql.spark.tile_generate import TileGenerate
from featurebyte.sql.spark.tile_monitor import TileMonitor
from featurebyte.sql.spark.tile_schedule_online_store import TileScheduleOnlineStore


class TileGenerateSchedule(TileCommon):
    offline_period_minute: int
    tile_start_date_column: str
    tile_last_start_date_column: str
    tile_start_date_placeholder: str
    tile_end_date_placeholder: str
    tile_type: str
    monitor_periods: int
    agg_id: str
    job_schedule_ts: str

    def execute(self) -> None:

        tile_end_ts = dateutil.parser.isoparse(self.job_schedule_ts)
        cron_residue_seconds = self.tile_modulo_frequency_second % 60
        tile_end_ts = tile_end_ts.replace(second=cron_residue_seconds)
        tile_end_ts = tile_end_ts - timedelta(seconds=self.blind_spot_second)

        tile_type = self.tile_type.upper()
        lookback_period = self.frequency_minute * (self.monitor_periods + 1)
        tile_id = self.tile_id.upper()

        if tile_type == "OFFLINE":
            lookback_period = self.offline_period_minute
            tile_end_ts = tile_end_ts - timedelta(minutes=lookback_period)

        tile_start_ts = tile_end_ts - timedelta(minutes=lookback_period)
        monitor_end_ts = tile_end_ts - timedelta(minutes=self.frequency_minute)

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
            '{self.agg_id}',
            '{tile_type}',
            '{session_id}',
            '<STATUS>',
            '<MESSAGE>',
            current_timestamp()
        )"""
        logger.debug(audit_insert_sql)

        insert_sql = audit_insert_sql.replace("<STATUS>", "STARTED").replace("<MESSAGE>", "")
        logger.debug(insert_sql)
        self._spark.sql(insert_sql)

        tile_start_ts_str = tile_start_ts.strftime("%Y-%m-%d %H:%M:%S")
        tile_end_ts_str = tile_end_ts.strftime("%Y-%m-%d %H:%M:%S")
        monitor_tile_end_ts_str = monitor_end_ts.strftime("%Y-%m-%d %H:%M:%S")

        monitor_input_sql = self.sql.replace(
            f"{self.tile_start_date_placeholder}", "'" + tile_start_ts_str + "'"
        ).replace(f"{self.tile_end_date_placeholder}", "'" + monitor_tile_end_ts_str + "'")

        generate_input_sql = self.sql.replace(
            f"{self.tile_start_date_placeholder}", "'" + tile_start_ts_str + "'"
        ).replace(f"{self.tile_end_date_placeholder}", "'" + tile_end_ts_str + "'")

        last_tile_start_ts = tile_end_ts - timedelta(minutes=self.frequency_minute)
        last_tile_start_str = last_tile_start_ts.strftime("%Y-%m-%d %H:%M:%S")

        tile_monitor_ins = TileMonitor(
            spark_session=self._spark,
            featurebyte_database="TEST_DB_1",
            tile_id=tile_id,
            tile_modulo_frequency_second=183,
            blind_spot_second=3,
            frequency_minute=5,
            sql=generate_input_sql,
            monitor_sql=monitor_input_sql,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_type="ONLINE",
            tile_start_date_column=InternalName.TILE_START_DATE,
        )

        tile_generate_ins = TileGenerate(
            spark_session=self._spark,
            featurebyte_database="TEST_DB_1",
            tile_id=tile_id,
            tile_modulo_frequency_second=183,
            blind_spot_second=3,
            frequency_minute=5,
            sql=generate_input_sql,
            entity_column_names=self.entity_column_names,
            value_column_names=self.value_column_names,
            value_column_types=self.value_column_types,
            tile_type="ONLINE",
            tile_start_date_column=InternalName.TILE_START_DATE,
            last_tile_start_str=last_tile_start_str,
            tile_last_start_date_column=self.tile_last_start_date_column,
        )

        tile_online_store_ins = TileScheduleOnlineStore(
            spark_session=self._spark,
            featurebyte_database="TEST_DB_1",
            agg_id=self.agg_id,
            job_schedule_ts_str=self.job_schedule_ts,
        )

        step_specs = [
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
                spec["trigger"].execute()
                logger.info(f"End of calling {spec['name']}\n")
            except Exception as e:
                message = str(e).replace("'", "")
                ex_insert_sql = audit_insert_sql.replace(
                    "<STATUS>", spec["status"]["fail"]
                ).replace(
                    "<MESSAGE>",
                    message,
                )
                logger.error("fail_insert_sql: ", ex_insert_sql)
                self._spark.sql(ex_insert_sql)
                raise e
            insert_sql = audit_insert_sql.replace("<STATUS>", spec["status"]["success"]).replace(
                "<MESSAGE>", ""
            )
            logger.info("success_insert_sql: ", insert_sql)
            self._spark.sql(insert_sql)
