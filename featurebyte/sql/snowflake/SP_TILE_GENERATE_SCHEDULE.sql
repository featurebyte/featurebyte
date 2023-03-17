CREATE OR REPLACE PROCEDURE SP_TILE_GENERATE_SCHEDULE(
    TILE_ID VARCHAR,
    AGGREGATION_ID VARCHAR,
    TIME_MODULO_FREQUENCY_SECONDS FLOAT,
    BLIND_SPOT_SECONDS FLOAT,
    FREQUENCY_MINUTE FLOAT,
    OFFLINE_PERIOD_MINUTE FLOAT,
    SQL VARCHAR,
    TILE_START_DATE_COLUMN VARCHAR,
    TILE_LAST_START_DATE_COLUMN VARCHAR,
    TILE_START_DATE_PLACEHOLDER VARCHAR,
    TILE_END_DATE_PLACEHOLDER VARCHAR,
    ENTITY_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_TYPES VARCHAR,
    TYPE VARCHAR,
    MONITOR_PERIODS FLOAT,
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
    var aggregation_id = AGGREGATION_ID.toUpperCase()
    var tile_type = TYPE.toUpperCase()

    var session_id = tile_id + "|" + new Date().toISOString()
    var audit_insert_sql = `
        INSERT INTO TILE_JOB_MONITOR (
          TILE_ID,
          AGGREGATION_ID,
          TILE_TYPE,
          SESSION_ID,
          STATUS,
          MESSAGE
        ) VALUES (
          '${tile_id}',
          '${aggregation_id}',
          '${tile_type}',
          '${session_id}',
          '<STATUS>',
          '<MESSAGE>'
        )
    `

    snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "STARTED").replace("<MESSAGE>", "")})

    // determine tile_end_ts (tile end timestamp) based on tile_type and JOB_SCHEDULE_TS
    var last_tile_end_ts = JOB_SCHEDULE_TS
    cron_residue_seconds = TIME_MODULO_FREQUENCY_SECONDS % 60
    last_tile_end_ts.setSeconds(cron_residue_seconds)
    last_tile_end_ts.setSeconds(last_tile_end_ts.getSeconds() - BLIND_SPOT_SECONDS)
    tile_end_ts = last_tile_end_ts

    debug = " - tile_end_ts: " + tile_end_ts

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
    monitor_tile_start_ts_str = tile_start_ts_str

    result = snowflake.execute({sqlText: `SELECT LAST_TILE_START_DATE_ONLINE FROM TILE_REGISTRY WHERE TILE_ID = '${tile_id}' AND LAST_TILE_START_DATE_ONLINE IS NOT NULL`})
    if (result.next()) {
        registry_last_tile_start_ts = result.getColumnValue(1).toISOString()
        debug = debug + " - registry_last_tile_start_ts: " + registry_last_tile_start_ts
        if (registry_last_tile_start_ts < tile_start_ts_str) {
            tile_start_ts_str = registry_last_tile_start_ts
        }
    }

    // trigger stored procedure to monitor previous tiles. No need to monitor the last tile to be created
    var monitor_end_ts = new Date(tile_end_ts.getTime())
    monitor_end_ts.setMinutes(monitor_end_ts.getMinutes() - FREQUENCY_MINUTE)
    monitor_tile_end_ts_str = monitor_end_ts.toISOString()
    debug = debug + " - monitor_tile_end_ts_str: " + monitor_tile_end_ts_str

    var monitor_input_sql = SQL.replaceAll(`${TILE_START_DATE_PLACEHOLDER}`, "''"+monitor_tile_start_ts_str+"''").replaceAll(`${TILE_END_DATE_PLACEHOLDER}`, "''"+monitor_tile_end_ts_str+"''")
    var monitor_stored_proc = `call SP_TILE_MONITOR('${monitor_input_sql}', '${TILE_START_DATE_COLUMN}', ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}, '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', '${VALUE_COLUMN_TYPES}', '${tile_id}', '${tile_type}')`
    try {
        result = snowflake.execute({sqlText: monitor_stored_proc})
        result.next()
        debug = debug + " - SP_TILE_MONITOR: " + result.getColumnValue(1)
    } catch (err)  {
        message = err.message.replaceAll("'", "")
        snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "MONITORED_FAILED").replace("<MESSAGE>", message)})
        throw err
    }
    snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "MONITORED").replace("<MESSAGE>", "")})

    // trigger stored procedure to generate tiles
    // replace SQL template with start and end date strings for tile generation sql
    var generate_input_sql = SQL.replaceAll(`${TILE_START_DATE_PLACEHOLDER}`, "''"+tile_start_ts_str+"''").replaceAll(`${TILE_END_DATE_PLACEHOLDER}`, "''"+tile_end_ts_str+"''")

    var last_tile_start_ts = new Date(tile_end_ts.getTime())
    last_tile_start_ts.setMinutes(last_tile_start_ts.getMinutes() - FREQUENCY_MINUTE)
    last_tile_start_ts_str = last_tile_start_ts.toISOString()

    var generate_stored_proc = `call SP_TILE_GENERATE('${generate_input_sql}', '${TILE_START_DATE_COLUMN}', '${TILE_LAST_START_DATE_COLUMN}', ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}, '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', '${VALUE_COLUMN_TYPES}', '${tile_id}', '${tile_type}', '${last_tile_start_ts_str}')`
    try {
        result = snowflake.execute({sqlText: generate_stored_proc})
        result.next()
        debug = debug + " - SP_TILE_GENERATE: " + result.getColumnValue(1)
    } catch (err)  {
        message = err.message.replaceAll("'", "")
        snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "GENERATED_FAILED").replace("<MESSAGE>", message)})
        throw err
    }
    snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "GENERATED").replace("<MESSAGE>", "")})

    // update related online feature store
    try {
        job_schedule_ts_str = last_tile_end_ts.toISOString()
        snowflake.execute({sqlText: `call SP_TILE_SCHEDULE_ONLINE_STORE('${AGGREGATION_ID}', '${job_schedule_ts_str}')`})
    } catch (err)  {
        message = err.message.replaceAll("'", "")
        snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "ONLINE_STORE_FAILED").replace("<MESSAGE>", message)})
        throw err
    }
    snowflake.execute({sqlText: audit_insert_sql.replace("<STATUS>", "COMPLETED").replace("<MESSAGE>", "")})

    return debug
$$;
