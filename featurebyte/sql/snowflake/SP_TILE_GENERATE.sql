CREATE OR REPLACE PROCEDURE SP_TILE_GENERATE(
    SQL VARCHAR,
    TILE_START_DATE_COLUMN VARCHAR,
    TILE_LAST_START_DATE_COLUMN VARCHAR,
    TIME_MODULO_FREQUENCY_SECOND FLOAT,
    BLIND_SPOT_SECOND FLOAT,
    FREQUENCY_MINUTE FLOAT,
    ENTITY_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_TYPES VARCHAR,
    TILE_ID VARCHAR,
    TILE_TYPE VARCHAR,
    LAST_TILE_START_STR VARCHAR,
    AGGREGATION_ID VARCHAR
)
returns string
language javascript
as
$$
    /*
        Stored Procedure to generate Tile records for the target Feature. This stored procedure will trigger the input SQL
        and create the Tile table if it doesn’t exist, or insert the tile records if the Tile table already exists.
    */

    var debug = "Debug"
    var tile_exist = "Y"
    var tile_id = TILE_ID.toUpperCase()

    try {
        snowflake.execute({sqlText: `SELECT * FROM ${tile_id} LIMIT 1`})
    } catch (err)  {
        tile_exist = "N"
    }
    debug = debug + " - tile_exist: " + tile_exist + " - " + `SELECT * FROM ${tile_id} LIMIT 1`


    var tile_sql = SQL.replaceAll("'", "''")
    snowflake.execute({sqlText: `
        CALL SP_TILE_REGISTRY(
            '${tile_sql}',
            ${TIME_MODULO_FREQUENCY_SECOND},
            ${BLIND_SPOT_SECOND},
            ${FREQUENCY_MINUTE},
            '${ENTITY_COLUMN_NAMES}',
            '${VALUE_COLUMN_NAMES}',
            '${VALUE_COLUMN_TYPES}',
            '${tile_id}',
            '${tile_id}',
            '${tile_exist}',
            '${AGGREGATION_ID}'
        )
    `})

    if (ENTITY_COLUMN_NAMES) {
        entity_column_names_select_str = `${ENTITY_COLUMN_NAMES},`
    } else {
        entity_column_names_select_str = ""
    }
    tile_sql = `
        select
          F_TIMESTAMP_TO_INDEX(
            ${TILE_START_DATE_COLUMN},
            ${TIME_MODULO_FREQUENCY_SECOND},
            ${BLIND_SPOT_SECOND},
            ${FREQUENCY_MINUTE}
          ) as INDEX,
          ${entity_column_names_select_str}
          ${VALUE_COLUMN_NAMES},
          SYSDATE() as CREATED_AT
        from (
          ${SQL}
        )
    `

    if (tile_exist === "N") {

        //feature tile table does not exist, create the table with the input tile sql
        var tile_create_sql = `create table ${tile_id} as ${tile_sql}`
        snowflake.execute({sqlText: tile_create_sql})
        debug = debug + " - tile_create_sql: " + tile_create_sql

    } else {

        //feature tile table already exists, insert tile records with the input tile sql
        entity_insert_cols = []
        entity_filter_cols = []
        for (const [i, element] of ENTITY_COLUMN_NAMES.split(",").entries()) {
            entity_insert_cols.push(`b.${element}`)
            entity_filter_cols.push(`EQUAL_NULL(a.${element}, b.${element})`)
        }
        entity_insert_cols_str = entity_insert_cols.join(",")
        entity_filter_cols_str = entity_filter_cols.join(" AND ")

        value_insert_cols = []
        value_update_cols = []
        for (const [i, element] of VALUE_COLUMN_NAMES.split(",").entries()) {
            value_insert_cols.push(`b.${element}`)
            value_update_cols.push(`a.${element} = b.${element}`)
        }
        value_insert_cols_str = value_insert_cols.join(",")
        value_update_cols_str = value_update_cols.join(",")

        if (ENTITY_COLUMN_NAMES) {
            on_condition_str = `a.INDEX = b.INDEX AND ${entity_filter_cols_str}`
            insert_str = `INDEX, ${ENTITY_COLUMN_NAMES}, ${VALUE_COLUMN_NAMES}, CREATED_AT`
            values_str = `b.INDEX, ${entity_insert_cols_str}, ${value_insert_cols_str}, SYSDATE()`
        }
        else {
            on_condition_str = `a.INDEX = b.INDEX`
            insert_str = `INDEX, ${VALUE_COLUMN_NAMES}, CREATED_AT`
            values_str = `b.INDEX, ${value_insert_cols_str}, SYSDATE()`
        }

        var tile_insert_sql = `
            merge into ${tile_id} a using (${tile_sql}) b
                on ${on_condition_str}
                when matched then
                    update set a.CREATED_AT = SYSDATE(), ${value_update_cols_str}
                when not matched then
                    insert (${insert_str})
                        values (${values_str})
        `
        snowflake.execute({sqlText: tile_insert_sql})
        debug = debug + " - tile_insert_sql: " + tile_insert_sql
    }

    if (LAST_TILE_START_STR != null) {
        // update last_tile index
        update_tile_last_ind_sql = `
            UPDATE TILE_REGISTRY
            SET LAST_TILE_INDEX_${TILE_TYPE} = F_TIMESTAMP_TO_INDEX('${LAST_TILE_START_STR}', ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, ${FREQUENCY_MINUTE}),
                ${TILE_LAST_START_DATE_COLUMN}_${TILE_TYPE} = '${LAST_TILE_START_STR}'
            WHERE TILE_ID = '${tile_id}'
            AND AGGREGATION_ID = '${AGGREGATION_ID}'
        `
        snowflake.execute({sqlText: update_tile_last_ind_sql})
    }

  return debug
$$;
