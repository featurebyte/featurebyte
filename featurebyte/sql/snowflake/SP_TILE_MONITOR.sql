CREATE OR REPLACE PROCEDURE SP_TILE_MONITOR(
    MONITOR_SQL VARCHAR,
    TILE_START_DATE_COLUMN VARCHAR,
    TIME_MODULO_FREQUENCY_SECOND FLOAT,
    BLIND_SPOT_SECOND FLOAT,
    FREQUENCY_MINUTE FLOAT,
    ENTITY_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_TYPES VARCHAR,
    TILE_ID VARCHAR,
    TILE_TYPE VARCHAR,
    AGGREGATION_ID VARCHAR
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

    var tile_exist = "Y"
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${TILE_ID} LIMIT 1`})
    } catch (err)  {
        tile_exist = "N"
    }
    debug = debug + " - online_tile_table_exist: " + tile_exist

    if (tile_exist === "N") {
        // first-time execution that online tile table does not exist yet
        return debug
    }

    var tile_sql = MONITOR_SQL.replaceAll("'", "''")
    var result = snowflake.execute({sqlText: `
        CALL SP_TILE_REGISTRY(
            '${tile_sql}',
            ${TIME_MODULO_FREQUENCY_SECOND},
            ${BLIND_SPOT_SECOND},
            ${FREQUENCY_MINUTE},
            '${ENTITY_COLUMN_NAMES}',
            '${VALUE_COLUMN_NAMES}',
            '${VALUE_COLUMN_TYPES}',
            '${TILE_ID}',
            '${TILE_ID}',
            'Y',
            '${AGGREGATION_ID}'
        )
    `})
    result.next()
    existing_value_columns = result.getColumnValue(1)
    debug = debug + " - existing_value_columns: " + existing_value_columns

    if (existing_value_columns === "") {
        // all are new value columns
        debug = debug + " - all value columns are new - col names: " + VALUE_COLUMN_NAMES
        debug = debug + " - col types: " + VALUE_COLUMN_TYPES
        return debug
    }

    entity_filter_cols = []
    for (const [i, element] of ENTITY_COLUMN_NAMES.split(",").entries()) {
        entity_filter_cols.push("a." + element + " = b."+ element)
    }
    entity_filter_cols_str = entity_filter_cols.join(" AND ")

    value_filter_cols = []
    value_select_cols = []
    for (const [i, element] of existing_value_columns.split(",").entries()) {
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
            F_TIMESTAMP_TO_INDEX(${TILE_START_DATE_COLUMN}, ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, ${FREQUENCY_MINUTE}) as INDEX,
            ${ENTITY_COLUMN_NAMES}, ${existing_value_columns}
        from (${MONITOR_SQL})
    `

    var compare_sql = `
        select * from
            (select
                a.*,
                ${value_select_cols_str},
                '${TILE_TYPE}'::VARCHAR(8) as TILE_TYPE,
                DATEADD(SECOND, (${BLIND_SPOT_SECOND}+${FREQUENCY_MINUTE}*60), a.${TILE_START_DATE_COLUMN}) as EXPECTED_CREATED_AT,
                SYSDATE() as CREATED_AT
            from
                (${new_tile_sql}) a left outer join ${TILE_ID} b
            on
                a.INDEX = b.INDEX AND ${entity_filter_cols_str})
        where ${value_filter_cols_str}
    `
    debug = debug + " - compare_sql: " + compare_sql

    var monitor_table_name = TILE_ID + '_MONITOR'
    table_exist = "Y"
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${monitor_table_name} LIMIT 1`})
    } catch (err)  {
        table_exist = "N"
    }
    debug = debug + " - monitor_table_exist: " + table_exist

    if (table_exist === "N") {
        // monitor table not exists, create table with new records
        var create_sql = `create table ${monitor_table_name} as ${compare_sql}`
        snowflake.execute({sqlText: create_sql})
        debug = debug + " - inside create monitor table"

    } else {

        snowflake.execute({sqlText: `
            CALL SP_TILE_REGISTRY(
                '${tile_sql}',
                ${TIME_MODULO_FREQUENCY_SECOND},
                ${BLIND_SPOT_SECOND},
                ${FREQUENCY_MINUTE},
                '${ENTITY_COLUMN_NAMES}',
                '${VALUE_COLUMN_NAMES}',
                '${VALUE_COLUMN_TYPES}',
                '${TILE_ID}',
                '${monitor_table_name}',
                'Y',
                '${AGGREGATION_ID}'
            )
        `})

        // monitor table already exists, insert new records
        var insert_sql = `
            insert into ${monitor_table_name}
                (${TILE_START_DATE_COLUMN}, INDEX, ${ENTITY_COLUMN_NAMES}, ${existing_value_columns}, EXPECTED_CREATED_AT, CREATED_AT)
                ${compare_sql}
        `
        snowflake.execute({sqlText: insert_sql})
        debug = debug + " - inside insert monitor records: " + insert_sql
    }


    var insert_monitor_summary_sql = `
        INSERT INTO TILE_MONITOR_SUMMARY(TILE_ID, TILE_START_DATE, TILE_TYPE)
        SELECT
            '${TILE_ID}' as TILE_ID,
            ${TILE_START_DATE_COLUMN} as TILE_START_DATE,
            TILE_TYPE
        FROM (${compare_sql})
    `
    snowflake.execute({sqlText: insert_monitor_summary_sql})
    debug = debug + " - insert_monitor_summary_sql: " + insert_monitor_summary_sql

    return debug
$$;
