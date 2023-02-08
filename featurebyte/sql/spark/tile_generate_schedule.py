"""
Tile Generate Schedule script for SP_TILE_GENERATE
"""
from datetime import datetime, timedelta

import dateutil.parser
import tile_generate
import tile_monitor
import tile_schedule_online_store
from tile_common import TileCommon


class TileGenerateSchedule(TileCommon):
    offline_period_minute: int
    tile_start_date_column: str
    tile_last_start_date_column: str
    tile_start_date_placeholder: str
    tile_end_date_placeholder: str

    tile_type: str
    monitor_periods: int
    job_schedule_ts: str

    def execute(self):

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
        audit_insert_sql = f"INSERT INTO TILE_JOB_MONITOR(TILE_ID, TILE_TYPE, SESSION_ID, STATUS, MESSAGE, CREATED_AT) VALUES ('{tile_id}', '{tile_type}', '{session_id}', '<STATUS>', '<MESSAGE>', current_timestamp())"
        print(audit_insert_sql)

        insert_sql = audit_insert_sql.replace("<STATUS>", "STARTED").replace("<MESSAGE>", "")
        print(insert_sql)
        self._spark.sql(insert_sql)

        tile_start_ts_str = tile_start_ts.strftime("%Y-%m-%d %H:%M:%S")
        tile_end_ts_str = tile_end_ts.strftime("%Y-%m-%d %H:%M:%S")
        monitor_tile_end_ts_str = monitor_end_ts.strftime("%Y-%m-%d %H:%M:%S")

        monitor_input_sql = self.sql.replace(
            f"{self.tile_start_date_placeholder}", "''" + tile_start_ts_str + "''"
        ).replace(f"{self.tile_end_date_placeholder}", "''" + monitor_tile_end_ts_str + "''")

        generate_input_sql = self.sql.replace(
            f"{self.tile_start_date_placeholder}", "''" + tile_start_ts_str + "''"
        ).replace(f"{self.tile_end_date_placeholder}", "''" + tile_end_ts_str + "''")

        last_tile_start_ts = tile_end_ts - timedelta(minutes=self.frequency_minute)
        last_tile_start_str = last_tile_start_ts.strftime("%Y-%m-%d %H:%M:%S")

        step_specs = [
            {
                "name": "tile_monitor",
                "data": {"monitor_sql": monitor_input_sql},
                "trigger": tile_monitor.main,
                "status": {
                    "fail": "MONITORED_FAILED",
                    "success": "MONITORED",
                },
            },
            {
                "name": "tile_generate",
                "data": {"sql": generate_input_sql, "last_tile_start_str": last_tile_start_str},
                "trigger": tile_generate.main,
                "status": {
                    "fail": "GENERATED_FAILED",
                    "success": "GENERATED",
                },
            },
            {
                "name": "tile_online_store",
                "data": {"job_schedule_ts_str": self.job_schedule_ts},
                "trigger": tile_schedule_online_store.main,
                "status": {
                    "fail": "ONLINE_STORE_FAILED",
                    "success": "COMPLETED",
                },
            },
        ]

        for spec in step_specs:
            new_args = self.dict()
            new_args.update(spec["data"])  # type: ignore[call-overload]

            try:
                print(f"Calling {spec['name']}\n")
                print(f"new_args: {new_args}\n")
                spec["trigger"](new_args)  # type: ignore[operator]
                print(f"End of calling {spec['name']}\n")
            except Exception as e:
                message = str(e).replace("'", "")
                ex_insert_sql = audit_insert_sql.replace(
                    "<STATUS>", spec["status"]["fail"]
                ).replace(
                    # type: ignore[index]
                    "<MESSAGE>",
                    message,
                )
                print("fail_insert_sql: ", ex_insert_sql)
                self._spark.sql(ex_insert_sql)
                raise e
            insert_sql = audit_insert_sql.replace("<STATUS>", spec["status"]["success"]).replace(  # type: ignore[index]
                "<MESSAGE>", ""
            )
            print("success_insert_sql: ", insert_sql)
            self._spark.sql(insert_sql)
