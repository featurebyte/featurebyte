"""
Databricks Tile Generate entity tracking Job Script
"""

import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TileManagement").getOrCreate()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("tile_last_start_date_column", type=str)
    parser.add_argument("entity_column_names", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("entity_table", type=str)

    args = parser.parse_args()

    featurebyte_database = args.featurebyte_database
    tile_last_start_date_column = args.tile_last_start_date_column
    entity_column_names = args.entity_column_names
    tile_id = args.tile_id.upper()
    entity_table = args.entity_table

    print("featurebyte_database: ", featurebyte_database)
    print("tile_last_start_date_column: ", tile_last_start_date_column)
    print("entity_column_names: ", entity_column_names)
    print("tile_id: ", tile_id)
    print("entity_table: ", entity_table)

    spark.sql(f"USE DATABASE {featurebyte_database}")

    tracking_table_name = tile_id + "_ENTITY_TRACKER"
    tracking_table_exist = spark.catalog.tableExists(tracking_table_name)

    entity_insert_cols = []
    entity_filter_cols = []
    for element in entity_column_names.split(","):
        element = element.strip()
        entity_insert_cols.append("b." + element)
        entity_filter_cols.append("a." + element + " = b." + element)

    entity_insert_cols_str = ",".join(entity_insert_cols)
    entity_filter_cols_str = " AND ".join(entity_filter_cols)
    print("entity_insert_cols_str: ", entity_insert_cols_str)
    print("entity_filter_cols_str: ", entity_filter_cols_str)

    # create table or insert new records or update existing records
    if not tracking_table_exist:
        create_sql = f"create table {tracking_table_name} as SELECT * FROM {entity_table}"
        print("create_sql: ", create_sql)
        spark.sql(create_sql)
    else:
        merge_sql = f"""
            merge into {tracking_table_name} a using {entity_table} b
                on {entity_filter_cols_str}
                when matched then
                    update set a.{tile_last_start_date_column} = b.{tile_last_start_date_column}
                when not matched then
                    insert ({entity_column_names}, {tile_last_start_date_column})
                        values ({entity_insert_cols_str}, b.{tile_last_start_date_column})
        """
        print("merge_sql: ", merge_sql)
        spark.sql(merge_sql)
