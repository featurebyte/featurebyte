create or replace procedure SP_TILE_GENERATE(SQL varchar, START_TS VARCHAR, END_TS VARCHAR, TABLE_NAME varchar, WINDOW_END_MINUTE float, FREQUENCY_MINUTE float)
returns string
language javascript
as
$$
    var debug = "Debug"

    //Check whether tile table for the feature already exists
    var tile_exist_sql = `SELECT exists (SELECT * FROM information_schema.tables WHERE table_name = '${TABLE_NAME}')`
    var result = snowflake.execute(
        {
            sqlText: tile_exist_sql
        }
    )
    result.next()
    var tile_exist = result.getColumnValue(1)
    debug = debug + " - tile_exist: " + tile_exist

    
    //replace SQL template with start and end date strings for tile generation sql
    //TEMPORARY for testing
    var tile_sql = SQL
    /*
    var tile_sql = SQL.replace("<start_ts>", START_TS).replace("<end_ts>", END_TS)
    tile_sql = `
        select 
            F_TIMESTAMP_TO_INDEX(event_timestamp, ${WINDOW_END_MINUTE}, ${FREQUENCY_MINUTE}) as INDEX, *
        from (${tile_sql})
    `
    */

    if (tile_exist == false) {
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
        //var tile_insert_sql = `insert into ${TABLE_NAME} ${tile_sql}`  
        var tile_insert_sql = `
            merge into ${TABLE_NAME} a using (${tile_sql}) b
                on a.INDEX = b.INDEX
                when matched then 
                    update set a.VALUE = b.VALUE
                when not matched then 
                    insert (index, event_timestamp, value) values (b.index, b.event_timestamp, b.value)
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
