"""
Databricks Tile Generate Job Script
"""

from typing import Any, Dict

import argparse

import tile_registry
from pyspark.sql import SparkSession


def main(args: Dict[str, Any]):

    spark = SparkSession.builder.appName("TileManagement").getOrCreate()
    featurebyte_database = args["featurebyte_database"]
    sql = args["sql"]
    tile_start_date_column = args["tile_start_date_column"]
    tile_last_start_date_column = args["tile_last_start_date_column"]
    tile_modulo_frequency_second = args["tile_modulo_frequency_second"]
    blind_spot_second = args["blind_spot_second"]
    frequency_minute = args["frequency_minute"]
    entity_column_names = args["entity_column_names"]
    value_column_names = args["value_column_names"]
    tile_id = args["tile_id"].upper()
    tile_type = args["tile_type"]
    last_tile_start_str = args["last_tile_start_str"]

    print("featurebyte_database: ", featurebyte_database)
    print("sql: ", sql)
    print("tile_start_date_column: ", tile_start_date_column)
    print("tile_last_start_date_column: ", tile_last_start_date_column)
    print("tile_modulo_frequency_second: ", tile_modulo_frequency_second)
    print("blind_spot_second: ", blind_spot_second)
    print("frequency_minute: ", frequency_minute)
    print("entity_column_names: ", entity_column_names)
    print("value_column_names: ", value_column_names)
    print("tile_id: ", tile_id)
    print("tile_type: ", tile_type)
    print("last_tile_start_str: ", last_tile_start_str)

    spark.sql(f"USE DATABASE {featurebyte_database}")

    tile_table_exist = spark.catalog.tableExists(tile_id)
    tile_table_exist_flag = "Y" if tile_table_exist else "N"

    # 2. Update TILE_REGISTRY & Add New Columns TILE Table

    tile_sql = sql.replace("'", "''")

    args["sql"] = tile_sql
    args["table_name"] = tile_id
    args["table_exist"] = tile_table_exist_flag

    print("\n\nCalling tile_registry.main\n")
    tile_registry.main(args)
    print("\nEnd of calling tile_registry.main\n\n")

    tile_sql = f"""
        select
            F_TIMESTAMP_TO_INDEX({tile_start_date_column}, {tile_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as index,
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

    # insert new records and update existing records
    if not tile_table_exist:
        print("creating tile table: ", tile_id)
        spark.sql(f"create table {tile_id} as {tile_sql}")
    else:
        merge_sql = f"""
            merge into {tile_id} a using ({tile_sql}) b
                on a.index = b.index AND {entity_filter_cols_str}
                when matched then
                    update set a.created_at = current_timestamp(), {value_update_cols_str}
                when not matched then
                    insert (index, {entity_column_names}, {value_column_names}, created_at)
                        values (b.index, {entity_insert_cols_str}, {value_insert_cols_str}, current_timestamp())
        """
        print("merging data: ", merge_sql)
        spark.sql(merge_sql)

    if last_tile_start_str:
        print("last_tile_start_str: ", last_tile_start_str)

        df = spark.sql(
            f"select F_TIMESTAMP_TO_INDEX('{last_tile_start_str}', {tile_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as value"
        )
        ind_value = df.select("value").collect()[0].value

        update_tile_last_ind_sql = f"""
            UPDATE TILE_REGISTRY SET LAST_TILE_INDEX_{tile_type} = {ind_value}, {tile_last_start_date_column}_{tile_type} = '{last_tile_start_str}'
            WHERE TILE_ID = '{tile_id}'
        """
        print(update_tile_last_ind_sql)
        spark.sql(update_tile_last_ind_sql)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("sql", type=str)
    parser.add_argument("tile_start_date_column", type=str)
    parser.add_argument("tile_last_start_date_column", type=str)
    parser.add_argument("tile_modulo_frequency_second", type=int)
    parser.add_argument("blind_spot_second", type=int)
    parser.add_argument("frequency_minute", type=int)
    parser.add_argument("entity_column_names", type=str)
    parser.add_argument("value_column_names", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("tile_type", type=str)
    parser.add_argument("last_tile_start_str", type=str)

    args = parser.parse_args()
    main(vars(args))
