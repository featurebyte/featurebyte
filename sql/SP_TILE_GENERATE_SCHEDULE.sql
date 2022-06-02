create or replace procedure SP_TILE_GENERATE_SCHEDULE(FEATURE_NAME varchar, TIME_MODULO_FREQUENCY_SECONDS float, BLIND_SPOT_SECONDS float, FREQUENCY_MINUTE float, OFFLINE_PERIOD_MINUTE float, SQL varchar, COLUMN_NAMES varchar, TYPE varchar, MONITOR_PERIODS float, END_TS varchar)
returns string
language javascript
as
$$
    /*
        Master Stored Procedure for scheduling and trigger Tile generation, monitoring and replacement

        1. derive cron_residue_seconds, window_end_seconds, start_ts and monitor_start_ts for online and offline Tile respectively
        2. call monitor tile stored procedure
        3. call generate tile stored procedure to create new or replace already existed tiles
    */

    var debug = "Debug"

    // derive and sleep cron_residue_seconds
    cron_residue_seconds = TIME_MODULO_FREQUENCY_SECONDS % 60
    snowflake.execute(
        {
            sqlText: `call system$wait(${cron_residue_seconds})`
        }
    )
    debug = debug + " - cron_residue_seconds: " + cron_residue_seconds

    window_end_seconds = TIME_MODULO_FREQUENCY_SECONDS - BLIND_SPOT_SECONDS
    debug = debug + " - window_end_seconds: " + window_end_seconds

    // determine tile_end_ts (tile end timestamp) based on tile_type and whether END_TS is set
    var tile_end_ts = new Date()
    if (END_TS != null) {
        debug = debug + " - END_TS: " + END_TS
        tile_end_ts = new Date(Date.parse(END_TS+' UTC'))

    } else {
        // Make sure tile_end_ts is at deterministic for each window
        tile_end_ts.setSeconds(cron_residue_seconds)
        tile_end_ts.setSeconds(tile_end_ts.getSeconds() - TIME_MODULO_FREQUENCY_SECONDS)
    }

    var tile_type = TYPE.toUpperCase()
    lookback_period = FREQUENCY_MINUTE * (MONITOR_PERIODS + 1)
    if (tile_type === "OFFLINE") {
        // offline schedule
        lookback_period = OFFLINE_PERIOD_MINUTE
        tile_end_ts.setMinutes(tile_end_ts.getMinutes() - lookback_period)
    }
    debug = debug + " - lookback_period: " + lookback_period

    tile_end_ts_str = tile_end_ts.toISOString().split("T")
    tile_end_ts_str = tile_end_ts_str[0] + " " + tile_end_ts_str[1].slice(0,8)
    debug = debug + " - tile_end_ts_str: " + tile_end_ts_str

    // trigger stored procedure to monitor previous tiles 
    var tile_start_ts = new Date(tile_end_ts.getTime())
    tile_start_ts.setMinutes(tile_start_ts.getMinutes() - lookback_period)

    tile_start_ts_str = tile_start_ts.toISOString().split("T")
    tile_start_ts_str = tile_start_ts_str[0] + " " + tile_start_ts_str[1].slice(0,8)
    debug = debug + " - tile_start_ts_str: " + tile_start_ts_str

    var input_sql = SQL.replace("FB_START_TS", "\\'"+tile_start_ts_str+"\\'::timestamp_ntz").replace("FB_END_TS", "\\'"+tile_end_ts_str+"\\'::timestamp_ntz")   
    var table_name = FEATURE_NAME.toUpperCase() + "_TILE"
    var monitor_stored_proc = `call SP_TILE_MONITOR('${input_sql}', ${window_end_seconds}, ${FREQUENCY_MINUTE}, '${COLUMN_NAMES}', '${table_name}', '${tile_type}')`
    result = snowflake.execute(
        {
            sqlText: monitor_stored_proc
        }
    )
    result.next()
    debug = debug + " - SP_TILE_MONITOR: " + result.getColumnValue(1)

    // trigger stored procedure to generate tiles
    var generate_stored_proc = `call SP_TILE_GENERATE('${input_sql}', ${window_end_seconds}, ${FREQUENCY_MINUTE}, '${COLUMN_NAMES}', '${table_name}')`
    var result = snowflake.execute(
        {
            sqlText: generate_stored_proc
        }
    )
    result.next()
    debug = debug + " - SP_TILE_GENERATE: " + result.getColumnValue(1)

    return debug
$$;
