CREATE OR REPLACE PROCEDURE SP_TILE_GENERATE_SCHEDULE(
    TILE_ID varchar,
    TIME_MODULO_FREQUENCY_SECONDS float,
    BLIND_SPOT_SECONDS float,
    FREQUENCY_MINUTE float,
    OFFLINE_PERIOD_MINUTE float,
    SQL varchar,
    TILE_START_DATE_COLUMN varchar,
    TILE_LAST_START_DATE_COLUMN varchar,
    TILE_START_DATE_PLACEHOLDER varchar,
    TILE_END_DATE_PLACEHOLDER varchar,
    ENTITY_COLUMN_NAMES varchar,
    VALUE_COLUMN_NAMES varchar,
    TYPE varchar,
    MONITOR_PERIODS float,
    JOB_SCHEDULE_TS timestamp_tz
)
returns string
language javascript
as
$$
    /*
        Master Stored Procedure to schedule and trigger Tile generation, monitoring and replacement

        1. derive cron_residue_seconds, tile_end_ts
        2. derive tile_start_ts for online and offline Tile respectively
        3. call monitor tile stored procedure
        4. call generate tile stored procedure to create new or replace already existed tiles
    */

    var debug = "Debug - JOB_SCHEDULE_TS: " + JOB_SCHEDULE_TS
    var tile_id = TILE_ID.toUpperCase()

    var session_id = tile_id+new Date().toISOString()
    session_id = session_id.split("").reduce(function(a,b){a=((a<<5)-a)+b.charCodeAt(0);return a&a},0)
    snowflake.execute({sqlText: `INSERT INTO TILE_JOB_MONITOR(TILE_ID, SESSION_ID, STATUS) VALUES ('${tile_id}', '${session_id}', 'STARTED')`})

    // determine tile_end_ts (tile end timestamp) based on tile_type and JOB_SCHEDULE_TS
    var tile_end_ts = JOB_SCHEDULE_TS
    cron_residue_seconds = TIME_MODULO_FREQUENCY_SECONDS % 60
    tile_end_ts.setSeconds(cron_residue_seconds)
    tile_end_ts.setSeconds(tile_end_ts.getSeconds() - BLIND_SPOT_SECONDS)

    debug = " - tile_end_ts: " + tile_end_ts

    var tile_type = TYPE.toUpperCase()
    var lookback_period = FREQUENCY_MINUTE * (MONITOR_PERIODS + 1)

    if (tile_type === "OFFLINE") {
        // offline schedule
        lookback_period = OFFLINE_PERIOD_MINUTE
        tile_end_ts.setMinutes(tile_end_ts.getMinutes() - lookback_period)
    }
    debug = debug + " - lookback_period_1: " + lookback_period

    tile_end_ts_str = tile_end_ts.toISOString()
    debug = debug + " - tile_end_ts_str: " + tile_end_ts_str

    var tile_start_ts = new Date(tile_end_ts.getTime())
    tile_start_ts.setMinutes(tile_start_ts.getMinutes() - lookback_period)
    tile_start_ts_str = tile_start_ts.toISOString()
    debug = debug + " - tile_start_ts_str: " + tile_start_ts_str

    // trigger stored procedure to monitor previous tiles. No need to monitor the last tile to be created
    var monitor_end_ts = new Date(tile_end_ts.getTime())
    monitor_end_ts.setMinutes(monitor_end_ts.getMinutes() - FREQUENCY_MINUTE)
    monitor_tile_end_ts_str = monitor_end_ts.toISOString()
    debug = debug + " - monitor_tile_end_ts_str: " + monitor_tile_end_ts_str

    var monitor_input_sql = SQL.replaceAll(`${TILE_START_DATE_PLACEHOLDER}`, "''"+tile_start_ts_str+"''").replaceAll(`${TILE_END_DATE_PLACEHOLDER}`, "''"+monitor_tile_end_ts_str+"''")
    var monitor_stored_proc = `call SP_TILE_MONITOR('${monitor_input_sql}', '${TILE_START_DATE_COLUMN}', ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}, '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', '${tile_id}', '${tile_type}')`
    result = snowflake.execute({sqlText: monitor_stored_proc})
    result.next()
    debug = debug + " - SP_TILE_MONITOR: " + result.getColumnValue(1)

    snowflake.execute({sqlText: `INSERT INTO TILE_JOB_MONITOR(TILE_ID, SESSION_ID, STATUS) VALUES ('${tile_id}', '${session_id}', 'MONITORED')`})

    // trigger stored procedure to generate tiles
    var generate_input_sql = SQL.replaceAll(`${TILE_START_DATE_PLACEHOLDER}`, "''"+tile_start_ts_str+"''").replaceAll(`${TILE_END_DATE_PLACEHOLDER}`, "''"+tile_end_ts_str+"''")

    var last_tile_start_ts = new Date(tile_end_ts.getTime())
    last_tile_start_ts.setMinutes(last_tile_start_ts.getMinutes() - FREQUENCY_MINUTE)
    last_tile_start_ts_str = last_tile_start_ts.toISOString()

    var generate_stored_proc = `call SP_TILE_GENERATE('${generate_input_sql}', '${TILE_START_DATE_COLUMN}', '${TILE_LAST_START_DATE_COLUMN}', ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}, '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', '${tile_id}', '${tile_type}', '${last_tile_start_ts_str}')`
    var result = snowflake.execute({sqlText: generate_stored_proc})
    result.next()
    debug = debug + " - SP_TILE_GENERATE: " + result.getColumnValue(1)

    snowflake.execute({sqlText: `INSERT INTO TILE_JOB_MONITOR(TILE_ID, SESSION_ID, STATUS) VALUES ('${tile_id}', '${session_id}', 'GENERATED')`})

    // update related online feature store
    job_schedule_ts_str = JOB_SCHEDULE_TS.toISOString()
    snowflake.execute({sqlText: `call SP_TILE_SCHEDULE_ONLINE_STORE('${tile_id}', '${job_schedule_ts_str}')`})

    snowflake.execute({sqlText: `INSERT INTO TILE_JOB_MONITOR(TILE_ID, SESSION_ID, STATUS) VALUES ('${tile_id}', '${session_id}', 'COMPLETED')`})

    return debug
$$;
