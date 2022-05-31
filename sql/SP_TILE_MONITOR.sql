create or replace procedure SP_TILE_MONITOR(MONITOR_SQL varchar, WINDOW_END_SECONDS float, FREQUENCY_MINUTE float, COLUMN_NAMES varchar, TABLE_NAME varchar)
returns string
language javascript
as
$$
    var debug = "Debug"

    var col_list = COLUMN_NAMES.split(",").filter(item => item.toUpperCase() !== "TILE_START_TS")
    col_list_str = col_list.join(',')
    debug = debug + " - col_list_str: " + col_list_str
    
    //replace SQL template with start and end date strings for tile generation sql 
    var new_tile_sql = `
        select 
            F_TIMESTAMP_TO_INDEX(TILE_START_TS, ${WINDOW_END_SECONDS}, ${FREQUENCY_MINUTE}) as INDEX, ${col_list_str}
        from (${MONITOR_SQL})
    `

    var compare_sql = `
        select t.*, m.VALUE as NEW_VALUE, sysdate() as CREATED_AT
        from ${TABLE_NAME} t, (${new_tile_sql}) m
        where t.INDEX = m.INDEX 
        and t.VALUE != m.VALUE
    `

    var monitor_table_name = TABLE_NAME + '_MONITOR'
    var table_exist_sql = `SELECT exists (SELECT * FROM information_schema.tables WHERE table_name = '${monitor_table_name}')`
    var result = snowflake.execute(
        {
            sqlText: table_exist_sql
        }
    )
    result.next()
    var table_exist = result.getColumnValue(1)
    debug = debug + " - monitor_table_exist: " + table_exist


    if (table_exist === false) {

        var create_sql = `create table ${monitor_table_name} as ${compare_sql}`
        snowflake.execute(
            {
                sqlText: create_sql
            }
        )
        debug = debug + " - inside create monitor table"

    } else {

        var insert_sql = `
            insert into ${monitor_table_name} ${compare_sql}
        ` 
        snowflake.execute(
            {
                sqlText: insert_sql
            }
        ) 
        debug = debug + " - inside insert monitor records"

    }

    return debug
$$;
