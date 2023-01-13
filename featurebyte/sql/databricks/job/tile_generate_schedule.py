"""
Databricks Tile Monitor Job Script
"""
import argparse
import copy
from datetime import datetime, timedelta

import dateutil.parser
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TileManagement").getOrCreate()
spark.sparkContext.addPyFile("dbfs:/FileStore/newudfs/tile_monitor.py")
spark.sparkContext.addPyFile("dbfs:/FileStore/newudfs/tile_generate.py")
spark.sparkContext.addPyFile("dbfs:/FileStore/newudfs/tile_schedule_online_store.py")

import tile_generate
import tile_monitor
import tile_schedule_online_store

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("tile_modulo_frequency_second", type=int)
    parser.add_argument("blind_spot_second", type=int)
    parser.add_argument("frequency_minute", type=int)
    parser.add_argument("offline_period_minute", type=int)
    parser.add_argument("sql", type=str)
    parser.add_argument("tile_start_date_column", type=str)
    parser.add_argument("tile_last_start_date_column", type=str)
    parser.add_argument("tile_start_date_placeholder", type=str)
    parser.add_argument("tile_end_date_placeholder", type=str)
    parser.add_argument("entity_column_names", type=str)
    parser.add_argument("value_column_names", type=str)
    parser.add_argument("tile_type", type=str)
    parser.add_argument("monitor_periods", type=int)
    parser.add_argument("job_schedule_ts", type=str)

    args = parser.parse_args()

    featurebyte_database = args.featurebyte_database
    tile_id = args.tile_id.upper()
    tile_modulo_frequency_second = args.tile_modulo_frequency_second
    blind_spot_second = args.blind_spot_second
    frequency_minute = args.frequency_minute
    offline_period_minute = args.offline_period_minute
    sql = args.sql
    tile_start_date_column = args.tile_start_date_column
    tile_last_start_date_column = args.tile_last_start_date_column
    tile_start_date_placeholder = args.tile_start_date_placeholder
    tile_end_date_placeholder = args.tile_end_date_placeholder
    entity_column_names = args.entity_column_names
    value_column_names = args.value_column_names
    tile_type = args.tile_type
    monitor_periods = args.monitor_periods
    job_schedule_ts = args.job_schedule_ts

    print("featurebyte_database: ", featurebyte_database)
    print("tile_id: ", tile_id)
    print("tile_modulo_frequency_second: ", tile_modulo_frequency_second)
    print("blind_spot_second: ", blind_spot_second)
    print("frequency_minute: ", frequency_minute)
    print("offline_period_minute: ", offline_period_minute)
    print("sql: ", sql)
    print("tile_start_date_coulmn: ", tile_start_date_column)
    print("tile_last_start_date_coulmn: ", tile_last_start_date_column)
    print("tile_start_date_placeholder: ", tile_start_date_placeholder)
    print("tile_end_date_placeholder: ", tile_end_date_placeholder)
    print("entity_column_names: ", entity_column_names)
    print("value_column_names: ", value_column_names)
    print("tile_type: ", tile_type)
    print("monitor_periods: ", monitor_periods)
    print("job_schedule_ts: ", job_schedule_ts)

    tile_end_ts = dateutil.parser.isoparse(job_schedule_ts)
    cron_residue_seconds = tile_modulo_frequency_second % 60
    tile_end_ts = tile_end_ts.replace(second=cron_residue_seconds)
    tile_end_ts = tile_end_ts - timedelta(seconds=blind_spot_second)

    tile_type = tile_type.upper()
    lookback_period = frequency_minute * (monitor_periods + 1)
    tile_id = tile_id.upper()

    if tile_type == "OFFLINE":
        lookback_period = offline_period_minute
        tile_end_ts = tile_end_ts - timedelta(minutes=lookback_period)

    tile_start_ts = tile_end_ts - timedelta(minutes=lookback_period)
    monitor_end_ts = tile_end_ts - timedelta(minutes=frequency_minute)

    spark.sql(f"USE DATABASE {featurebyte_database}")

    session_id = f"{tile_id}|{datetime.now()}"
    audit_insert_sql = f"INSERT INTO TILE_JOB_MONITOR(TILE_ID, TILE_TYPE, SESSION_ID, STATUS, MESSAGE, CREATED_AT) VALUES ('{tile_id}', '{tile_type}', '{session_id}', '<STATUS>', '<MESSAGE>', current_timestamp())"
    print(audit_insert_sql)

    insert_sql = audit_insert_sql.replace("<STATUS>", "STARTED").replace("<MESSAGE>", "")
    print(insert_sql)
    spark.sql(insert_sql)

    tile_start_ts_str = tile_start_ts.strftime("%Y-%m-%d %H:%M:%S")
    tile_end_ts_str = tile_end_ts.strftime("%Y-%m-%d %H:%M:%S")
    monitor_tile_end_ts_str = monitor_end_ts.strftime("%Y-%m-%d %H:%M:%S")

    monitor_input_sql = sql.replace(
        f"{tile_start_date_placeholder}", "''" + tile_start_ts_str + "''"
    ).replace(f"{tile_end_date_placeholder}", "''" + monitor_tile_end_ts_str + "''")

    generate_input_sql = sql.replace(
        f"{tile_start_date_placeholder}", "''" + tile_start_ts_str + "''"
    ).replace(f"{tile_end_date_placeholder}", "''" + tile_end_ts_str + "''")

    last_tile_start_ts = tile_end_ts - timedelta(minutes=frequency_minute)
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
            "data": {"job_schedule_ts_str": job_schedule_ts},
            "trigger": tile_schedule_online_store.main,
            "status": {
                "fail": "ONLINE_STORE_FAILED",
                "success": "COMPLETED",
            },
        },
    ]

    for spec in step_specs:
        new_args = copy.deepcopy(vars(args))
        new_args.update(spec["data"])  # type: ignore[call-overload]

        try:
            print(f"Calling {spec['name']}\n")
            print(f"new_args: {new_args}\n")
            spec["trigger"](new_args)  # type: ignore[operator]
            print(f"End of calling {spec['name']}\n")
        except Exception as e:
            message = str(e).replace("'", "")
            ex_insert_sql = audit_insert_sql.replace("<STATUS>", spec["status"]["fail"]).replace(  # type: ignore[index]
                "<MESSAGE>", message
            )
            print("fail_insert_sql: ", ex_insert_sql)
            spark.sql(ex_insert_sql)
            raise e
        insert_sql = audit_insert_sql.replace("<STATUS>", spec["status"]["success"]).replace(  # type: ignore[index]
            "<MESSAGE>", ""
        )
        print("success_insert_sql: ", insert_sql)
        spark.sql(insert_sql)
