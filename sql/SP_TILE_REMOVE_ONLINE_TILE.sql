create or replace procedure SP_TILE_REMOVE_ONLINE_TILE(OFFLINE_END_TS varchar, WINDOW_END_SECONDS float, FREQUENCY_MINUTE float, TABLE_NAME_PREFIX varchar)
returns string
language javascript
as
$$
    /*
        Stored Procedure to remove Stale Online Tiles after Offline Tiles are generated
    */
    
    var debug = "Debug"

    var table_name = TABLE_NAME_PREFIX + "ONLINE"
    var delete_sql = `
        delete from ${table_name} where F_INDEX_TO_TIMESTAMP(INDEX, ${WINDOW_END_SECONDS}, ${FREQUENCY_MINUTE}) < '${OFFLINE_END_TS}'
    ` 
    snowflake.execute(
        {
            sqlText: delete_sql
        }
    )
    debug = debug + " - delete_sql: " + delete_sql

    return debug
$$;
