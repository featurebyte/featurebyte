"""
Databricks Tile Generate Job Script
"""

import argparse

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("TileGenerate").getOrCreate()

    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("sql", type=str)
    parser.add_argument("tile_start_date_coulmn", type=str)
    parser.add_argument("tile_last_start_date_coulmn", type=str)
    parser.add_argument("tile_modulo_frequency_second", type=int)
    parser.add_argument("blind_spot_second", type=int)
    parser.add_argument("frequency_minute", type=int)
    parser.add_argument("entity_column_names", type=str)
    parser.add_argument("value_column_names", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("tile_type", type=str)
    parser.add_argument("last_tile_start_str", type=str)

    args = parser.parse_args()

    featurebyte_database = args.featurebyte_database
    sql = args.sql
    tile_start_date_coulmn = args.tile_start_date_coulmn
    tile_last_start_date_coulmn = args.tile_last_start_date_coulmn
    tile_modulo_frequency_second = args.tile_modulo_frequency_second
    blind_spot_second = args.blind_spot_second
    frequency_minute = args.frequency_minute
    entity_column_names = args.entity_column_names
    value_column_names = args.value_column_names
    tile_id = args.tile_id
    tile_type = args.tile_type
    last_tile_start_str = args.last_tile_start_str

    print("featurebyte_database: ", args.featurebyte_database)
    print("sql: ", args.sql)
    print("tile_start_date_coulmn: ", args.tile_start_date_coulmn)
    print("tile_last_start_date_coulmn: ", args.tile_last_start_date_coulmn)
    print("tile_modulo_frequency_second: ", args.tile_modulo_frequency_second)
    print("blind_spot_second: ", args.blind_spot_second)
    print("frequency_minute: ", args.frequency_minute)
    print("entity_column_names: ", args.entity_column_names)
    print("value_column_names: ", args.value_column_names)
    print("tile_id: ", args.tile_id)
    print("tile_type: ", args.tile_type)
    print("last_tile_start_str: ", args.last_tile_start_str)

    spark.sql(f"USE DATABASE {args.featurebyte_database}")

    tile_registry_create_sql = """
        CREATE TABLE IF NOT EXISTS TILE_REGISTRY (
            TILE_ID VARCHAR(16777216),
            TILE_SQL VARCHAR(16777216),
            ENTITY_COLUMN_NAMES VARCHAR(16777216),
            VALUE_COLUMN_NAMES VARCHAR(16777216),
            FREQUENCY_MINUTE int,
            TIME_MODULO_FREQUENCY_SECOND int,
            BLIND_SPOT_SECOND int,
            IS_ENABLED BOOLEAN,
            LAST_TILE_START_DATE_ONLINE timestamp,
            LAST_TILE_INDEX_ONLINE int,
            LAST_TILE_START_DATE_OFFLINE timestamp,
            LAST_TILE_INDEX_OFFLINE int,
            CREATED_AT timestamp
        )
    """
    spark.sql(tile_registry_create_sql)

    tile_table_exist = spark.catalog.tableExists(tile_id)

    # 2. Update TILE_REGISTRY & Add New Columns TILE Table

    ### 2.1. Derive new VALUE columns

    new_cols = set()

    if tile_table_exist:
        existing_cols = {c.name for c in spark.catalog.listColumns(tile_id)}
        input_cols = set(value_column_names.split(","))
        new_cols = input_cols.difference(existing_cols)
        print("new_cols: ", new_cols)

        # when tile table exist, add new columns to the tile table when there is new column
        for new_c in new_cols:
            spark.sql(f"alter table {tile_id} add column {new_c} double")

    ### 2.2. Update TILE_REGISTRY

    if tile_table_exist:
        if new_cols:
            new_cols_str = "," + ",".join(new_cols)
            update_sql = f"update tile_registry set VALUE_COLUMN_NAMES = concat(VALUE_COLUMN_NAMES, '{new_cols_str}') where tile_id = '{tile_id}'"
            spark.sql(update_sql)
    else:
        df = spark.sql(f"select count(*) as value from tile_registry where tile_id = '{tile_id}'")
        ind_value = df.select("value").collect()[0].value
        print("ind_value: ", ind_value)

        if ind_value == 0:
            escape_sql = sql.replace("'", "''")
            insert_sql = f"""
                insert into tile_registry(
                    TILE_ID, TILE_SQL, ENTITY_COLUMN_NAMES, VALUE_COLUMN_NAMES, FREQUENCY_MINUTE, TIME_MODULO_FREQUENCY_SECOND,
                    BLIND_SPOT_SECOND, IS_ENABLED, CREATED_AT,
                    LAST_TILE_START_DATE_ONLINE, LAST_TILE_INDEX_ONLINE, LAST_TILE_START_DATE_OFFLINE, LAST_TILE_INDEX_OFFLINE
                )
                VALUES (
                    '{tile_id}', '{escape_sql}', '{entity_column_names}', '{value_column_names}', {frequency_minute}, {tile_modulo_frequency_second},
                    {blind_spot_second}, TRUE, current_timestamp(),
                    null, null, null, null
                )
            """
            print("insert_sql: ", insert_sql)
            spark.sql(insert_sql)

    ### 2.3. Derive TILE Create or Merge SQL

    tile_sql = f"""
    select
                F_TIMESTAMP_TO_INDEX({tile_start_date_coulmn}, {tile_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as index,
                {entity_column_names}, {value_column_names},
                current_timestamp() as created_at
            from ({sql})
    """

    print("tile_sql:", tile_sql)

    entity_insert_cols = []
    entity_filter_cols = []
    for element in entity_column_names.split(","):
        element = element.strip()
        entity_insert_cols.append("b." + element)
        entity_filter_cols.append("a." + element + " = b." + element)

    entity_insert_cols_str = ",".join(entity_insert_cols)
    entity_filter_cols_str = " AND ".join(entity_filter_cols)

    value_insert_cols = []
    value_update_cols = []
    for element in value_column_names.split(","):
        element = element.strip()
        value_insert_cols.append("b." + element)
        value_update_cols.append("a." + element + " = b." + element)

    value_insert_cols_str = ",".join(value_insert_cols)
    value_update_cols_str = ",".join(value_update_cols)

    print("entity_insert_cols_str: ", entity_insert_cols_str)
    print("entity_filter_cols_str: ", entity_filter_cols_str)
    print("value_insert_cols_str: ", value_insert_cols_str)
    print("value_update_cols_str: ", value_update_cols_str)

    merge_sql = f"""
        merge into {tile_id} a using ({tile_sql}) b
            on a.index = b.index AND {entity_filter_cols_str}
            when matched then
                update set a.created_at = current_timestamp(), {value_update_cols_str}
            when not matched then
                insert (index, {entity_column_names}, {value_column_names}, created_at)
                    values (b.index, {entity_insert_cols_str}, {value_insert_cols_str}, current_timestamp())
    """

    print("merge_sql:", merge_sql)

    ### 2.4. Create or Merge TILE table

    # insert new records and update existing records
    if not tile_table_exist:
        spark.sql(f"create table {tile_id} as {tile_sql}")
    else:
        print("merging data: ", merge_sql)
        spark.sql(merge_sql)

    if last_tile_start_str:
        print("last_tile_start_str: ", last_tile_start_str)

        df = spark.sql(
            f"select F_TIMESTAMP_TO_INDEX('{last_tile_start_str}', {tile_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as value"
        )
        ind_value = df.select("value").collect()[0].value

        update_tile_last_ind_sql = f"""
            UPDATE TILE_REGISTRY SET LAST_TILE_INDEX_{tile_type} = {ind_value}, LAST_TILE_START_DATE_{tile_type} = '{last_tile_start_str}'
            WHERE TILE_ID = '{tile_id}'
        """
        print(update_tile_last_ind_sql)
        spark.sql(update_tile_last_ind_sql)
