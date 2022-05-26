create or replace procedure SP_TILE_GENERATE_SCHEDULE(FEATURE_NAME varchar, WINDOW_END_MINUTE float, BLINDSPOT_RESIDUE_SECONDS float, FREQUENCY_MINUTE float, SQL varchar, COLUMN_NAMES varchar, TYPE varchar, MONITOR_PERIODS float, END_TS varchar)
returns string
language javascript
as
$$
    var debug = "Debug"

    sleep_seconds = BLINDSPOT_RESIDUE_SECONDS % 60
    snowflake.execute(
        {
            sqlText: `call system$wait(${sleep_seconds})`
        }
    );
    debug = debug + " - sleep_seconds: " + sleep_seconds

    var tile_type = TYPE.toUpperCase()
    var frequency_min = FREQUENCY_MINUTE

    var end_ts = new Date()
    if (END_TS != null) {
        debug = debug + " - END_TS: " + END_TS
        end_ts = new Date(Date.parse(END_TS+' UTC'))
    }

    if (tile_type === "OFFLINE") {
        // offline schedule
        frequency_min = 1440
        end_ts.setDate(end_ts.getDate() - 1)
    }

    var start_ts = new Date(end_ts.getTime())
    start_ts.setMinutes(start_ts.getMinutes() - frequency_min)

    var monitor_start_ts = new Date(end_ts.getTime())
    monitor_start_ts.setMinutes(monitor_start_ts.getMinutes() - frequency_min * MONITOR_PERIODS)

    end_ts_str = end_ts.toISOString().split("T")
    end_ts_str = end_ts_str[0] + " " + end_ts_str[1].slice(0,5)+":00"
    debug = debug + " - end_ts_str: " + end_ts_str

    start_ts_str = start_ts.toISOString().split("T")
    start_ts_str = start_ts_str[0] + " " + start_ts_str[1].slice(0,5)+":00"
    debug = debug + " - start_ts_str: " + start_ts_str

    monitor_start_ts_str = monitor_start_ts.toISOString().split("T")
    monitor_start_ts_str = monitor_start_ts_str[0] + " " + monitor_start_ts_str[1].slice(0,5)+":00"
    debug = debug + " - monitor_start_ts_str: " + monitor_start_ts_str

    var table_name = FEATURE_NAME.toUpperCase() + "_TILE_" + tile_type
    var input_sql = SQL.replace("FB_START_TS", "\\'"+start_ts_str+"\\'").replace("FB_END_TS", "\\'"+end_ts_str+"\\'")
    var generate_stored_proc = `call SP_TILE_GENERATE('${input_sql}', ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE}, '${COLUMN_NAMES}', '${table_name}')`
    var result = snowflake.execute(
        {
            sqlText: generate_stored_proc
        }
    )
    result.next()
    debug = debug + " - SP_TILE_GENERATE: " + result.getColumnValue(1)

    var monitor_input_sql = SQL.replace("FB_START_TS", "\\'"+monitor_start_ts_str+"\\'").replace("FB_END_TS", "\\'"+end_ts_str+"\\'")
    var monitor_stored_proc = `call SP_TILE_MONITOR('${monitor_input_sql}', ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE}, '${COLUMN_NAMES}', '${table_name}')`
    result = snowflake.execute(
        {
            sqlText: monitor_stored_proc
        }
    )
    result.next()
    debug = debug + " - SP_TILE_MONITOR: " + result.getColumnValue(1)

    return debug
$$;
