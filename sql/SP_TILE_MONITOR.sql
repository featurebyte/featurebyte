create or replace procedure SP_TILE_MONITOR(MONITOR_SQL varchar, WINDOW_END_SECONDS float, FREQUENCY_MINUTE float, COLUMN_NAMES varchar, TABLE_NAME_PREFIX varchar, TILE_TYPE varchar)
returns string
language javascript
as
$$
    /*
        Stored Procedure to Monitor Tile records. The stored procedure will construct and trigger the MONITOR_SQL and compare 
        the result with the already generated TILE values. Any difference found in the record values will be inserted into the tile monitor table
    */

    var debug = "Debug"
    
    var col_list = COLUMN_NAMES.split(",").filter(item => item.trim().toUpperCase() !== "TILE_START_TS")
    col_list_str = col_list.join(',')
    debug = debug + " - col_list_str: " + col_list_str

    filter_cols_str = ""
    insert_cols_str = ""
    for (element of col_list) {
        element = element.trim()
        if (element.toUpperCase() !== 'VALUE') {
            filter_cols_str = filter_cols_str + " AND a." + element + " = b."+ element
            insert_cols_str = insert_cols_str + "b." + element + ","
        }
    }
    insert_cols_str = insert_cols_str.slice(0, -1)    

    //replace SQL template with start and end date strings for tile generation sql 
    var new_tile_sql = `
        select 
            F_TIMESTAMP_TO_INDEX(TILE_START_TS, ${WINDOW_END_SECONDS}, ${FREQUENCY_MINUTE}) as INDEX, ${col_list_str}
        from (${MONITOR_SQL})
    `

    var online_table_name = TABLE_NAME_PREFIX + "ONLINE"
    var compare_sql = `
        select a.*, b.VALUE as NEW_VALUE, sysdate() as CREATED_AT
        from ${online_table_name} a, (${new_tile_sql}) b
        where a.INDEX = b.INDEX ${filter_cols_str}
        and a.VALUE != b.VALUE
    `
    debug = debug + " - compare_sql: " + compare_sql

    var monitor_table_name = TABLE_NAME_PREFIX + TILE_TYPE + '_MONITOR'
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
        // monitor table already exists, create table with new records
        var create_sql = `create table ${monitor_table_name} as ${compare_sql}`
        snowflake.execute(
            {
                sqlText: create_sql
            }
        )
        debug = debug + " - inside create monitor table"

    } else {
        // monitor table already exists, insert new records

        var insert_sql = `
            merge into ${monitor_table_name} a using (${compare_sql}) b
                on a.INDEX = b.INDEX and a.NEW_VALUE = b.NEW_VALUE and a.VALUE = b.VALUE ${filter_cols_str}
                when not matched then 
                    insert values (b.INDEX, ${insert_cols_str}, b.VALUE, b.NEW_VALUE, b.CREATED_AT)
        `
        
        snowflake.execute(
            {
                sqlText: insert_sql
            }
        ) 
        debug = debug + " - inside insert monitor records: " + insert_sql

    }

    return debug
$$;
