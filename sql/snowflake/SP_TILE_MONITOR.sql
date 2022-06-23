CREATE OR REPLACE PROCEDURE SP_TILE_MONITOR(MONITOR_SQL varchar, TIME_MODULO_FREQUENCY_SECONDS float, BLIND_SPOT_SECONDS float, FREQUENCY_MINUTE float, COLUMN_NAMES varchar, TABLE_NAME varchar, TILE_TYPE varchar)
returns string
language javascript
as
$$
    /*
        Stored Procedure to Monitor Tile records. The stored procedure will construct and trigger the MONITOR_SQL and compare
        the result with the already generated TILE values. Any difference found in the record values will be inserted into the tile monitor table
    */

    var debug = "Debug"

    var table_exist = true
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${TABLE_NAME} LIMIT 1`})
    } catch (err)  {
        table_exist = false
    }
    debug = debug + " - online_tile_table_exist: " + table_exist


    if (table_exist === false) {
        // first-time execution that online tile table does not exist yet
        return debug
    }

    var col_list = COLUMN_NAMES.split(",").filter(item => item.trim().toUpperCase() !== "TILE_START_TS")
    col_list_str = col_list.join(',')
    debug = debug + " - col_list_str: " + col_list_str

    filter_cols_str = ""
    for (element of col_list) {
        element = element.trim()
        if (element.toUpperCase() !== 'VALUE') {
            filter_cols_str = filter_cols_str + " AND a." + element + " = b."+ element
        }
    }

    //replace SQL template with start and end date strings for tile generation sql
    var new_tile_sql = `
        select
            TILE_START_TS,
            F_TIMESTAMP_TO_INDEX(TILE_START_TS, ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}) as INDEX,
            ${col_list_str}
        from (${MONITOR_SQL})
    `

    var compare_sql = `
        select * from
            (select
                a.*,
                b.VALUE as OLD_VALUE,
                '${TILE_TYPE}'::VARCHAR(8) as TILE_TYPE,
                DATEADD(SECOND, (${BLIND_SPOT_SECONDS}+${FREQUENCY_MINUTE}*60), a.TILE_START_TS) as EXPECTED_CREATED_AT,
                SYSDATE() as CREATED_AT
            from
                (${new_tile_sql}) a left outer join ${TABLE_NAME} b
            on
                a.INDEX = b.INDEX ${filter_cols_str})
        where VALUE != OLD_VALUE or OLD_VALUE IS NULL
    `
    debug = debug + " - compare_sql: " + compare_sql

    var monitor_table_name = TABLE_NAME + '_MONITOR'
    table_exist = true
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${monitor_table_name} LIMIT 1`})
    } catch (err)  {
        table_exist = false
    }
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
            insert into ${monitor_table_name} ${compare_sql}
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
