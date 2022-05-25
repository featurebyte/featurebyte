create or replace procedure SP_TILE_GENERATE_SCHEDULE(FEATURE_NAME varchar, WINDOW_END_MINUTE float, BLINDSPOT_SECONDS float, FREQUENCY_MINUTE float, SQL varchar, TYPE varchar)
returns string
language javascript
as
$$
    var debug = "Debug";

    sleep_seconds = BLINDSPOT_SECONDS % 60
    snowflake.execute(
        {
            sqlText: `call system$wait(${sleep_seconds})`
        }
    );
    debug = debug + " - sleep_seconds: " + sleep_seconds

    var tile_type = TYPE.toUpperCase()
    var frequency_min = FREQUENCY_MINUTE
    if (tile_type != "ONLINE") {
        // offline schedule 
        frequency_min = 1440
    }

    
    end_ts = new Date()
    end_ts_str = end_ts.toISOString().split("T")
    end_ts_str = end_ts_str[0] + " " + end_ts_str[1].slice(0,5)+":00"
    debug = debug + " - end_ts_str: " + end_ts_str

    start_ts = end_ts
    start_ts.setMinutes(start_ts.getMinutes() - frequency_min)
    start_ts_str = start_ts.toISOString().split("T")
    start_ts_str = start_ts_str[0] + " " + start_ts_str[1].slice(0,5)+":00"
    debug = debug + " - start_ts_str: " + start_ts_str

    var table_name = FEATURE_NAME.toUpperCase() + "_TILE" + "_" + tile_type
    var tile_create_sql = `call SP_TILE_GENERATE('${SQL}', '${start_ts_str}', '${end_ts_str}', '${table_name}', ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE})`
    var result = snowflake.execute(
        {
            sqlText: tile_create_sql
        }
    )
    result.next()
    debug = debug + " - SP_TILE_GENERATE: " + result.getColumnValue(1) 

    return debug
$$;
