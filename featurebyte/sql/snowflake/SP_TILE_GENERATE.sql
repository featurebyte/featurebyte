CREATE OR REPLACE PROCEDURE SP_TILE_GENERATE(
    SQL varchar,
    TILE_START_DATE_COLUMN varchar,
    TIME_MODULO_FREQUENCY_SECOND float,
    BLIND_SPOT_SECOND float,
    FREQUENCY_MINUTE float,
    ENTITY_COLUMN_NAMES varchar,
    VALUE_COLUMN_NAMES varchar,
    TILE_ID varchar,
    TILE_TYPE varchar,
    LAST_TILE_START_STR varchar
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

    //check whether the tile registry record already exists
    var result = snowflake.execute({sqlText: `SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '${TILE_ID}'`})
    if (result.getRowCount() === 0) {
        // no registry record exists, insert a new registry record
        var tile_sql = SQL.replaceAll("'", "''")
        var insert_sql = `
            INSERT INTO TILE_REGISTRY (TILE_ID, TILE_SQL, ENTITY_COLUMN_NAMES, VALUE_COLUMN_NAMES, FREQUENCY_MINUTE, TIME_MODULO_FREQUENCY_SECOND, BLIND_SPOT_SECOND, IS_ENABLED)
            VALUES ('${TILE_ID}', '${tile_sql}', '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', ${FREQUENCY_MINUTE}, ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, False)
        `
        snowflake.execute({sqlText: insert_sql})
        debug = debug + " - inserted new tile registry record with " + insert_sql
    }

    //check whether tile table already exists
    var tile_exist = true
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${TILE_ID} LIMIT 1`})
    } catch (err)  {
        tile_exist = false
    }
    debug = debug + " - tile_exist: " + tile_exist


    //replace SQL template with start and end date strings for tile generation sql
    var tile_sql = `
        select
            F_TIMESTAMP_TO_INDEX(${TILE_START_DATE_COLUMN}, ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, ${FREQUENCY_MINUTE}) as INDEX,
            ${ENTITY_COLUMN_NAMES}, ${VALUE_COLUMN_NAMES},
            SYSDATE() as CREATED_AT
        from (${SQL})
    `

    if (tile_exist === false) {

        //feature tile table does not exist, create the table with the input tile sql
        var tile_create_sql = `create table ${TILE_ID} as ${tile_sql}`
        snowflake.execute(
            {
                sqlText: tile_create_sql
            }
        )
        debug = debug + " - tile_create_sql: " + tile_create_sql

    } else {

        //feature tile table already exists, insert tile records with the input tile sql
        entity_insert_cols = []
        entity_filter_cols = []
        for (const [i, element] of ENTITY_COLUMN_NAMES.split(",").entries()) {

            entity_insert_cols.push("b."+element)
            entity_filter_cols.push("a." + element + " = b."+ element)

        }
        entity_insert_cols_str = entity_insert_cols.join(",")
        entity_filter_cols_str = entity_filter_cols.join(" AND ")

        value_insert_cols = []
        value_update_cols = []
        for (const [i, element] of VALUE_COLUMN_NAMES.split(",").entries()) {

            value_insert_cols.push("b."+element)
            value_update_cols.push("a."+element+" = b."+element)

        }
        value_insert_cols_str = value_insert_cols.join(",")
        value_update_cols_str = value_update_cols.join(",")


        var tile_insert_sql = `
            merge into ${TILE_ID} a using (${tile_sql}) b
                on a.INDEX = b.INDEX AND ${entity_filter_cols_str}
                when matched then
                    update set a.CREATED_AT = SYSDATE(), ${value_update_cols_str}
                when not matched then
                    insert (INDEX, ${ENTITY_COLUMN_NAMES}, ${VALUE_COLUMN_NAMES}, CREATED_AT)
                        values (b.INDEX, ${entity_insert_cols_str}, ${value_insert_cols_str}, SYSDATE())
        `
        snowflake.execute(
            {
                sqlText: tile_insert_sql
            }
        )
        debug = debug + " - tile_insert_sql: " + tile_insert_sql
    }

    if (LAST_TILE_START_STR != null) {
        // update last_tile index
        update_tile_last_ind_sql = `
            UPDATE TILE_REGISTRY
            SET LAST_TILE_INDEX_${TILE_TYPE} = F_TIMESTAMP_TO_INDEX('${LAST_TILE_START_STR}', ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, ${FREQUENCY_MINUTE}),
                LAST_TILE_START_DATE_${TILE_TYPE} = '${LAST_TILE_START_STR}'
            WHERE TILE_ID = '${TILE_ID}'
        `
        snowflake.execute({sqlText: update_tile_last_ind_sql})
    }

  return debug
$$;
