CREATE OR REPLACE PROCEDURE SP_TILE_REGISTRY(
    SQL varchar,
    TIME_MODULO_FREQUENCY_SECOND float,
    BLIND_SPOT_SECOND float,
    FREQUENCY_MINUTE float,
    ENTITY_COLUMN_NAMES varchar,
    VALUE_COLUMN_NAMES varchar,
    TILE_ID varchar,
    TABLE_NAME varchar,
    TABLE_EXIST varchar
)
returns string
language javascript
as
$$
    /*
        Stored Procedure to register tile entry and add new columns if exists
    */

    var debug = "Debug"
    var r_result = ""

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
    } else {
        snowflake.execute({sqlText: `UPDATE TILE_REGISTRY SET VALUE_COLUMN_NAMES = '${VALUE_COLUMN_NAMES}' WHERE TILE_ID = '${TILE_ID}'`})
    }

    //check whether tile table already exists and add new value columns for the tile or monitor table
    if(TABLE_EXIST == "Y") {

        input_value_columns = VALUE_COLUMN_NAMES.split(",")

        result = snowflake.execute({sqlText: `SHOW COLUMNS IN ${TABLE_NAME}`})
        var table_columns = []
        while (result.next())  {
            table_columns.push(result.getColumnValue(3))
        }

        // add new value columns for the tile table
        var tile_add_sql = `ALTER TABLE ${TABLE_NAME} ADD\n`
        var add_statements = []
        var existing_value_columns = []

        for (const [i, element] of input_value_columns.entries()) {
            if (table_columns.includes(element)) {
                existing_value_columns.push(element)
                continue
            }

            add_statements.push(`${element} REAL DEFAULT NULL`)
            if (TABLE_NAME.includes("_MONITOR")) {
                add_statements.push(`OLD_${element} REAL DEFAULT NULL`)
            }
        }

        if (add_statements.length > 0) {
            tile_add_sql += add_statements.join(",\n")
            debug = debug + " - tile_add_sql: " + tile_add_sql
            try {
                snowflake.execute({sqlText: tile_add_sql})
            } catch (err)  {
                debug = debug + " - warning: " + err
            }
        }

        r_result = existing_value_columns.join(",")
    }

    return r_result
$$;
