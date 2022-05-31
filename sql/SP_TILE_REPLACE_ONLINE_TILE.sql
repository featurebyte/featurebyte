create or replace procedure SP_TILE_REPLACE_ONLINE_TILE(OFFLINE_END_TS varchar, WINDOW_END_MINUTE float, FREQUENCY_MINUTE float, TABLE_NAME varchar)
returns string
language javascript
as
$$
    var debug = "Debug"

    var delete_sql = `
        delete from ${TABLE_NAME} where F_TIMESTAMP_TO_INDEX(TILE_START_TS, ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE}) < '${OFFLINE_END_TS}'
    ` 
    snowflake.execute(
        {
        sqlText: insert_sql
        }
    ) 

    return debug
$$;
