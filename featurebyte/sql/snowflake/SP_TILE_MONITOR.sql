CREATE OR REPLACE PROCEDURE SP_TILE_MONITOR(
    MONITOR_SQL varchar,
    TILE_START_DATE_COLUMN varchar,
    TIME_MODULO_FREQUENCY_SECONDS float,
    BLIND_SPOT_SECONDS float,
    FREQUENCY_MINUTE float,
    ENTITY_COLUMN_NAMES varchar,
    VALUE_COLUMN_NAMES varchar,
    TABLE_NAME varchar,
    TILE_TYPE varchar
)
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

    entity_filter_cols = []
    for (const [i, element] of ENTITY_COLUMN_NAMES.split(",").entries()) {
        entity_filter_cols.push("a." + element + " = b."+ element)
    }
    entity_filter_cols_str = entity_filter_cols.join(" AND ")

    value_filter_cols = []
    value_select_cols = []
    for (const [i, element] of VALUE_COLUMN_NAMES.split(",").entries()) {
        value_filter_cols.push(element + " != OLD_"+ element)
        value_filter_cols.push("(" + element + " IS NOT NULL AND OLD_" + element + " IS NULL)")
        value_select_cols.push("b." + element + " as OLD_" + element)

    }
    value_filter_cols_str = value_filter_cols.join(" OR ")
    value_select_cols_str = value_select_cols.join(" , ")

    //replace SQL template with start and end date strings for tile generation sql
    var new_tile_sql = `
        select
            ${TILE_START_DATE_COLUMN},
            F_TIMESTAMP_TO_INDEX(${TILE_START_DATE_COLUMN}, ${TIME_MODULO_FREQUENCY_SECONDS}, ${BLIND_SPOT_SECONDS}, ${FREQUENCY_MINUTE}) as INDEX,
            ${ENTITY_COLUMN_NAMES}, ${VALUE_COLUMN_NAMES}
        from (${MONITOR_SQL})
    `

    var compare_sql = `
        select * from
            (select
                a.*,
                ${value_select_cols_str},
                '${TILE_TYPE}'::VARCHAR(8) as TILE_TYPE,
                DATEADD(SECOND, (${BLIND_SPOT_SECONDS}+${FREQUENCY_MINUTE}*60), a.${TILE_START_DATE_COLUMN}) as EXPECTED_CREATED_AT,
                SYSDATE() as CREATED_AT
            from
                (${new_tile_sql}) a left outer join ${TABLE_NAME} b
            on
                a.INDEX = b.INDEX AND ${entity_filter_cols_str})
        where ${value_filter_cols_str}
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
