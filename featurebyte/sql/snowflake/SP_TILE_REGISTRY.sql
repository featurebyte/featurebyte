CREATE OR REPLACE PROCEDURE SP_TILE_REGISTRY(
    SQL VARCHAR,
    TIME_MODULO_FREQUENCY_SECOND float,
    BLIND_SPOT_SECOND float,
    FREQUENCY_MINUTE float,
    ENTITY_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_NAMES VARCHAR,
    VALUE_COLUMN_TYPES VARCHAR,
    TILE_ID VARCHAR,
    TABLE_NAME VARCHAR,
    TABLE_EXIST VARCHAR,
    AGGREGATION_ID VARCHAR
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

    var result = snowflake.execute({sqlText: `
        SELECT VALUE_COLUMN_NAMES, VALUE_COLUMN_TYPES
        FROM TILE_REGISTRY
        WHERE TILE_ID = '${TILE_ID}'
        AND AGGREGATION_ID = '${AGGREGATION_ID}'
    `})
    if (result.getRowCount() === 0) {
        // no registry record exists, insert a new registry record
        var tile_sql = SQL.replaceAll("'", "''")
        var insert_sql = `
            INSERT INTO TILE_REGISTRY
            (
              TILE_ID,
              AGGREGATION_ID,
              TILE_SQL,
              ENTITY_COLUMN_NAMES,
              VALUE_COLUMN_NAMES,
              VALUE_COLUMN_TYPES,
              FREQUENCY_MINUTE,
              TIME_MODULO_FREQUENCY_SECOND,
              BLIND_SPOT_SECOND,
              IS_ENABLED
            )
            VALUES
            (
              '${TILE_ID}',
              '${AGGREGATION_ID}',
              '${tile_sql}',
              '${ENTITY_COLUMN_NAMES}',
              '${VALUE_COLUMN_NAMES}',
              '${VALUE_COLUMN_TYPES}',
              ${FREQUENCY_MINUTE},
              ${TIME_MODULO_FREQUENCY_SECOND},
              ${BLIND_SPOT_SECOND},
              False
            )
        `
        snowflake.execute({sqlText: insert_sql})
        debug = debug + " - inserted new tile registry record with " + insert_sql
    } else {
        result.next()

        exist_value_columns = result.getColumnValue(1).split(",")
        exist_value_column_types = result.getColumnValue(2).split(",")

        input_value_columns = VALUE_COLUMN_NAMES.split(",")
        input_value_column_types = VALUE_COLUMN_TYPES.split(",")

        for (const [i, element] of input_value_columns.entries()) {
            if (!exist_value_columns.includes(element)) {
                exist_value_columns.push(element)
            }

            element_type = input_value_column_types[i]
            if (!exist_value_column_types.includes(element_type)) {
                exist_value_column_types.push(element_type)
            }
        }

        // there are new value columns - update tile registry
        new_value_columns_str = exist_value_columns.join(",")
        new_value_column_types_str = exist_value_column_types.join(",")

        snowflake.execute({sqlText: `UPDATE TILE_REGISTRY SET VALUE_COLUMN_NAMES = '${new_value_columns_str}', VALUE_COLUMN_TYPES = '${new_value_column_types_str}'  WHERE TILE_ID = '${TILE_ID}'`})
    }

    //check whether tile table already exists and add new value columns for the tile or monitor table
    if(TABLE_EXIST == "Y") {

        input_value_columns = VALUE_COLUMN_NAMES.split(",")
        input_value_column_types = VALUE_COLUMN_TYPES.split(",")

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

            element_type = input_value_column_types[i]
            add_statements.push(`${element} ${element_type} DEFAULT NULL`)
            if (TABLE_NAME.includes("_MONITOR")) {
                add_statements.push(`OLD_${element} ${element_type} DEFAULT NULL`)
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
