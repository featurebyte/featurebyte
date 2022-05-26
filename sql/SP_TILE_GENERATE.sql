create or replace procedure SP_TILE_GENERATE(SQL varchar, WINDOW_END_MINUTE float, FREQUENCY_MINUTE float, COLUMN_NAMES VARCHAR, START_TS VARCHAR, END_TS VARCHAR, TABLE_NAME varchar)
returns string
language javascript
as
$$
    var debug = "Debug"

    //check whether tile table for the feature already exists
    var tile_exist_sql = `SELECT exists (SELECT * FROM information_schema.tables WHERE table_name = '${TABLE_NAME}')`
    var result = snowflake.execute(
        {
            sqlText: tile_exist_sql
        }
    )
    result.next()
    var tile_exist = result.getColumnValue(1)
    debug = debug + " - tile_exist: " + tile_exist

    col_list = COLUMN_NAMES.split(",").filter(item => item.toUpperCase() !== "TILE_START_TS")
    col_list_str = col_list.join(',')
    debug = debug + " - col_list_str: " + col_list_str
    
    //replace SQL template with start and end date strings for tile generation sql    
    var tile_sql = SQL.replace("FB_START_TS", START_TS).replace("FB_END_TS", END_TS)
    tile_sql = `
        select 
            F_TIMESTAMP_TO_INDEX(TILE_START_TS, ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE}) as INDEX, ${col_list_str}
        from (${tile_sql})
    `

    if (tile_exist === false) {

        //feature tile table does not exist, create the table with the input tile sql
        var tile_create_sql = `create table ${TABLE_NAME} as ${tile_sql}`
        snowflake.execute(
            {
                sqlText: tile_create_sql
            }
        )
        debug = debug + " - inside create tile table"

    } else {

        //feature tile table already exists, insert tile records with the input tile sql
        insert_cols_str = ""
        for (element of col_list) {
          insert_cols_str = insert_cols_str + "b." + element + ","
        }
        insert_cols_str = insert_cols_str.slice(0, -1)

        var tile_insert_sql = `
            merge into ${TABLE_NAME} a using (${tile_sql}) b
                on a.INDEX = b.INDEX
                when matched then 
                    update set a.VALUE = b.VALUE
                when not matched then 
                    insert (INDEX, ${col_list_str}) values (b.INDEX, ${insert_cols_str})
        ` 
        snowflake.execute(
            {
                sqlText: tile_insert_sql
            }
        ) 
        debug = debug + " - inside insert tile records"

    }

    return debug
$$;
