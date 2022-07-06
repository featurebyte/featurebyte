CREATE OR REPLACE PROCEDURE SP_TILE_GENERATE(
    SQL varchar,
    TIME_MODULO_FREQUENCY_SECOND float,
    BLIND_SPOT_SECOND float,
    FREQUENCY_MINUTE float,
    COLUMN_NAMES varchar,
    TILE_ID varchar,
    TILE_TYPE varchar,
    LAST_TILE_START_STR varchar,
    TILE_START_DATE_COLUMN varchar
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
        var tile_sql = SQL.replaceAll("'", "\\'")
        var insert_sql = `
            INSERT INTO TILE_REGISTRY (TILE_ID, TILE_SQL, COLUMN_NAMES, FREQUENCY_MINUTE, TIME_MODULO_FREQUENCY_SECOND, BLIND_SPOT_SECOND, IS_ENABLED)
            VALUES ('${TILE_ID}', '${tile_sql}', '${COLUMN_NAMES}', ${FREQUENCY_MINUTE}, ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, False)
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

    var col_list = COLUMN_NAMES.split(",").filter(item => item.trim().toUpperCase() !== `${TILE_START_DATE_COLUMN}`)
    col_list_str = col_list.join(',')
    debug = debug + " - col_list_str: " + col_list_str

    //replace SQL template with start and end date strings for tile generation sql
    var tile_sql = `
        select
            F_TIMESTAMP_TO_INDEX(${TILE_START_DATE_COLUMN}, ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, ${FREQUENCY_MINUTE}) as INDEX,
            ${col_list_str},
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
        insert_cols_str = ""
        filter_cols_str = ""
        for (element of col_list) {
            element = element.trim()
            insert_cols_str = insert_cols_str + "b." + element + ","
            if (element.toUpperCase() !== 'VALUE') {
                filter_cols_str = filter_cols_str + " AND a." + element + " = b."+ element
            }
        }
        insert_cols_str = insert_cols_str.slice(0, -1)

        var tile_insert_sql = `
            merge into ${TILE_ID} a using (${tile_sql}) b
                on a.INDEX = b.INDEX ${filter_cols_str}
                when matched then
                    update set a.VALUE = b.VALUE, a.CREATED_AT = SYSDATE()
                when not matched then
                    insert (INDEX, ${col_list_str}, CREATED_AT) values (b.INDEX, ${insert_cols_str}, SYSDATE())
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
