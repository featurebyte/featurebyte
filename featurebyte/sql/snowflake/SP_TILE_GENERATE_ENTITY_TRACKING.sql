CREATE OR REPLACE PROCEDURE SP_TILE_GENERATE_ENTITY_TRACKING(
    TILE_ID varchar,
    ENTITY_COLUMN_NAMES varchar,
    ENTITY_TABLE varchar,
    TILE_LAST_START_DATE_COLUMN varchar
)
returns string
language javascript
as
$$
    /*
        Stored Procedure to generate tile tracking records of last_tile_start_date for request
    */

    var debug = "Debug"
    var tile_tracking_table = TILE_ID + "_ENTITY_TRACKER"

    debug = debug + " - ENTITY_COLUMN_NAMES: " + ENTITY_COLUMN_NAMES

    //check whether enity table already exists
    var table_exist = true
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${tile_tracking_table} LIMIT 1`})
    } catch (err)  {
        table_exist = false
    }
    debug = debug + " - table_exist: " + table_exist + " - tile_tracking_table: " + tile_tracking_table

    if (table_exist === false) {

        //table does not exist, create the table with the input table
        var create_sql = `create table ${tile_tracking_table} as SELECT * FROM (${ENTITY_TABLE})`
        snowflake.execute({sqlText: create_sql})
        debug = debug + " - create_sql: " + create_sql

    } else {

        col_list = ENTITY_COLUMN_NAMES.split(",")

        insert_cols = []
        filter_cols = []
        for (const [i, element] of col_list.entries()) {

            insert_cols.push("b."+element)
            filter_cols.push(`EQUAL_NULL(a.${element}, b.${element})`)
        }
        insert_cols_str = insert_cols.join(",")
        filter_cols_str = filter_cols.join(" AND ")

        var insert_sql = `
            merge into ${tile_tracking_table} a using (${ENTITY_TABLE}) b
                on ${filter_cols_str}
                when matched then
                    update set a.${TILE_LAST_START_DATE_COLUMN} = b.${TILE_LAST_START_DATE_COLUMN}
                when not matched then
                    insert (${ENTITY_COLUMN_NAMES}, ${TILE_LAST_START_DATE_COLUMN}) values (${insert_cols_str}, b.${TILE_LAST_START_DATE_COLUMN})
        `
        snowflake.execute({sqlText: insert_sql})
        debug = debug + " - insert_sql: " + insert_sql
    }

  return debug
$$;
