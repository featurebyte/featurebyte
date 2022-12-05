CREATE OR REPLACE PROCEDURE SP_TILE_SCHEDULE_ONLINE_STORE (
    TILE_ID varchar,
    JOB_SCHEDULE_TS_STR varchar
)
returns string
language javascript
as
$$
    /*
        Stored Procedure to update records in the online feature store

        1. use tile_id to retrieve records including feature_sql and feature_store_table_name from the tile_feature_mapping table
        2. merge updated values into the feature_store_table
    */

    var debug = "Debug - TILE_ID: " + TILE_ID


    var select_sql = `
        SELECT FEATURE_NAME, FEATURE_SQL, FEATURE_STORE_TABLE_NAME, FEATURE_ENTITY_COLUMN_NAMES, FEATURE_TYPE
        FROM TILE_FEATURE_MAPPING WHERE TILE_ID ILIKE '${TILE_ID}' AND IS_DELETED = FALSE
    `
    var result = snowflake.execute({sqlText: select_sql})
    var table_columns = []
    while (result.next())  {

        var f_name = result.getColumnValue(1)
        var f_sql = result.getColumnValue(2)
        var fs_table = result.getColumnValue(3)
        var f_entity_columns = result.getColumnValue(4)
        var f_value_type = result.getColumnValue(5)

        f_sql = f_sql.replaceAll("__FB_POINT_IN_TIME_SQL_PLACEHOLDER", "'" + JOB_SCHEDULE_TS_STR + "'")

        var table_exist = "Y"
        try {
            snowflake.execute({sqlText: `SELECT * FROM ${fs_table} LIMIT 1`})
        } catch (err)  {
            table_exist = "N"
        }
        debug = debug + " - table_exist: " + table_exist

        if (table_exist === "N") {

            //feature store table does not exist, create table with the input feature sql
            var create_sql = `create table ${fs_table} as ${f_sql}`
            snowflake.execute({sqlText: create_sql})
            debug = debug + " - create_sql: " + create_sql

        } else {

            //feature store table already exists, insert records with the input feature sql
            entity_insert_cols = []
            entity_filter_cols = []
            for (const [i, element] of f_entity_columns.split(",").entries()) {

                entity_insert_cols.push("b."+element)
                entity_filter_cols.push("a." + element + " = b."+ element)

            }
            entity_insert_cols_str = entity_insert_cols.join(",")
            entity_filter_cols_str = entity_filter_cols.join(" AND ")

            // check whether feature value column exists, if not add the new column
            try {
                snowflake.execute({sqlText: `SELECT "${f_name}" FROM ${fs_table} LIMIT 1`})
            } catch (err)  {
                snowflake.execute({sqlText: `ALTER TABLE ${fs_table} ADD "${f_name}" ${f_value_type}`})
            }

            // remove feature values for entities that are not in entity universe
            var remove_values_sql = `
                update ${fs_table} set "${f_name}" = NULL
                    where (${f_entity_columns}) not in
                    (select ${f_entity_columns} from (${f_sql}))
            `
            debug = debug + " - remove_values_sql: " + remove_values_sql
            snowflake.execute({sqlText: remove_values_sql})

            // update or insert feature values for entities that are in entity universe
            var merge_sql = `
                merge into ${fs_table} a using (${f_sql}) b
                    on ${entity_filter_cols_str}
                    when matched then
                        update set a."${f_name}" = b."${f_name}"
                    when not matched then
                        insert (${f_entity_columns}, "${f_name}")
                            values (${entity_insert_cols_str}, "${f_name}")
            `
            debug = debug + " - merge_sql: " + merge_sql
            snowflake.execute({sqlText: merge_sql})
        }
    }

    return debug
$$;
