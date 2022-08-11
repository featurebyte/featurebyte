CREATE OR REPLACE PROCEDURE SP_TILE_REGISTRY(
    SQL varchar,
    TIME_MODULO_FREQUENCY_SECOND float,
    BLIND_SPOT_SECOND float,
    FREQUENCY_MINUTE float,
    ENTITY_COLUMN_NAMES varchar,
    VALUE_COLUMN_NAMES varchar,
    TILE_ID varchar,
    TABLE_NAME varchar
)
returns boolean
language javascript
as
$$
    /*
        Stored Procedure to register tile entry and add new columns if exists
    */

    var debug = "Debug"
    var new_value_columns = []

    //check whether the tile registry record already exists
    var result = snowflake.execute({sqlText: `SELECT VALUE_COLUMN_NAMES FROM TILE_REGISTRY WHERE TILE_ID = '${TILE_ID}'`})
    if (result.getRowCount() === 0) {
        // no registry record exists, insert a new registry record
        var tile_sql = SQL.replaceAll("'", "''")
        var insert_sql = `
            INSERT INTO TILE_REGISTRY (TILE_ID, TILE_SQL, ENTITY_COLUMN_NAMES, VALUE_COLUMN_NAMES, FREQUENCY_MINUTE, TIME_MODULO_FREQUENCY_SECOND, BLIND_SPOT_SECOND, IS_ENABLED)
            VALUES ('${TILE_ID}', '${tile_sql}', '${ENTITY_COLUMN_NAMES}', '${VALUE_COLUMN_NAMES}', ${FREQUENCY_MINUTE}, ${TIME_MODULO_FREQUENCY_SECOND}, ${BLIND_SPOT_SECOND}, False)
        `
        snowflake.execute({sqlText: insert_sql})
        debug = debug + " - inserted new tile registry record with " + insert_sql

        new_value_columns = VALUE_COLUMN_NAMES.split(",")
    } else {
        result.next()

        exist_value_columns = result.getColumnValue(1).split(",")
        input_value_columns = VALUE_COLUMN_NAMES.split(",")

        for (const [i, element] of input_value_columns.entries()) {
            if (!exist_value_columns.includes(element)) {
                new_value_columns.push(element)
            }
        }

        if(new_value_columns.length > 0) {
            // there are new value columns - update tile registry
            new_value_columns_str = (exist_value_columns.concat(new_value_columns)).join(",")
            snowflake.execute({sqlText: `UPDATE TILE_REGISTRY SET VALUE_COLUMN_NAMES = '${new_value_columns_str}' WHERE TILE_ID = '${TILE_ID}'`})
        }
    }

    //check whether tile table already exists and add new value columns for the tile table
    var tile_exist = true
    try {
        snowflake.execute({sqlText: `SELECT * FROM ${TABLE_NAME} LIMIT 1`})

        if(new_value_columns.length > 0) {
            // add new value columns for the tile table
            var tile_add_sql = `ALTER TABLE ${TABLE_NAME} ADD\n`
            add_statements = []
            for (const [i, element] of new_value_columns.entries()) {
                add_statements.push(`${element} REAL DEFAULT NULL`)
            }
            tile_add_sql += add_statements.join(",\n")
            snowflake.execute({sqlText: tile_add_sql})
            debug = debug + " - tile_add_sql: " + tile_add_sql
        }
    } catch (err)  {
        tile_exist = false
    }
    debug = debug + " - tile_exist: " + tile_exist

    return tile_exist
$$;
