create or replace procedure SP_TILE_GENERATE_SCHEDULE(FEATURE_NAME varchar, TIME_MODULO_FREQUENCY_SECONDS float, BLIND_SPOT_SECONDS float, FREQUENCY_MINUTE float, SQL varchar, COLUMN_NAMES varchar, TYPE varchar, MONITOR_PERIODS float, END_TS varchar)
returns string
language javascript
as
$$
    /*
        Master Stored Procedure for scheduling and trigger Tile generation, monitoring and replacement

        1. derive cron_residue_seconds, window_end_seconds, start_ts and monitor_start_ts for online and offline Tile respectively
        2. call generate tile stored procedure 
        3. call monitor tile stored procedure
        4. if tile_type is offline, remove stale online tiles
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

    // determine end_ts based on tile_type and whether END_TS is set
    var end_ts = new Date()
    if (END_TS != null) {
        debug = debug + " - END_TS: " + END_TS
        end_ts = new Date(Date.parse(END_TS+' UTC'))
    }

    var tile_type = TYPE.toUpperCase()
    if (tile_type === "OFFLINE") {
        // offline schedule
        end_ts.setDate(end_ts.getMinutes() - FREQUENCY_MINUTE)
    }

    end_ts_str = end_ts.toISOString().split("T")
    end_ts_str = end_ts_str[0] + " " + end_ts_str[1].slice(0,8)
    debug = debug + " - end_ts_str: " + end_ts_str

    // trigger stored procedure to generate tiles
    var start_ts = new Date(end_ts.getTime())
    start_ts.setMinutes(start_ts.getMinutes() - FREQUENCY_MINUTE)
    start_ts_str = start_ts.toISOString().split("T")
    start_ts_str = start_ts_str[0] + " " + start_ts_str[1].slice(0,8)
    debug = debug + " - start_ts_str: " + start_ts_str

    var table_name = FEATURE_NAME.toUpperCase() + "_TILE_" + tile_type
    var input_sql = SQL.replace("FB_START_TS", "\\'"+start_ts_str+"\\'::timestamp_ntz").replace("FB_END_TS", "\\'"+end_ts_str+"\\'::timestamp_ntz")
    var generate_stored_proc = `call SP_TILE_GENERATE('${input_sql}', ${window_end_seconds}, ${FREQUENCY_MINUTE}, '${COLUMN_NAMES}', '${table_name}')`
    var result = snowflake.execute(
        {
            sqlText: generate_stored_proc
        }
    )
    result.next()
    debug = debug + " - SP_TILE_GENERATE: " + result.getColumnValue(1)

    // trigger stored procedure to monitor tiles
    var monitor_start_ts = new Date(end_ts.getTime())
    monitor_start_ts.setMinutes(monitor_start_ts.getMinutes() - FREQUENCY_MINUTE * MONITOR_PERIODS)
    monitor_start_ts_str = monitor_start_ts.toISOString().split("T")
    monitor_start_ts_str = monitor_start_ts_str[0] + " " + monitor_start_ts_str[1].slice(0,8)
    debug = debug + " - monitor_start_ts_str: " + monitor_start_ts_str

    var monitor_input_sql = SQL.replace("FB_START_TS", "\\'"+monitor_start_ts_str+"\\'::timestamp_ntz").replace("FB_END_TS", "\\'"+end_ts_str+"\\'::timestamp_ntz")
    var monitor_stored_proc = `call SP_TILE_MONITOR('${monitor_input_sql}', ${window_end_seconds}, ${FREQUENCY_MINUTE}, '${COLUMN_NAMES}', '${table_name}')`
    result = snowflake.execute(
        {
            sqlText: monitor_stored_proc
        }
    )
    result.next()
    debug = debug + " - SP_TILE_MONITOR: " + result.getColumnValue(1)

    if (tile_type === "OFFLINE") {
        // remove stale online tiles whose tile_start_ts is less than end_ts
        var table_name = FEATURE_NAME.toUpperCase() + "_TILE_ONLINE"
        var stored_proc = `call SP_TILE_REPLACE_ONLINE_TILE('${end_ts}', ${window_end_seconds}, ${FREQUENCY_MINUTE}, '${table_name}')`
        result = snowflake.execute(
            {
                sqlText: stored_proc
            }
        )
        result.next()
        debug = debug + " - SP_TILE_REPLACE_ONLINE_TILE: " + result.getColumnValue(1)
    }

    return debug
$$;
