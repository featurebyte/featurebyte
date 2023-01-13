"""
Databricks Tile Generate online store Job Script
"""

from typing import Any, Dict

import argparse

from pyspark.sql import SparkSession


def main(args: Dict[str, Any]) -> None:

    spark = SparkSession.builder.appName("TileManagement").getOrCreate()

    featurebyte_database = args["featurebyte_database"]
    tile_id = args["tile_id"].upper()
    job_schedule_ts_str = args["job_schedule_ts_str"]

    print("featurebyte_database: ", featurebyte_database)
    print("tile_id: ", tile_id)
    print("job_schedule_ts_str: ", job_schedule_ts_str)

    spark.sql(f"use database {featurebyte_database}")

    select_sql = f"""
        SELECT FEATURE_NAME, FEATURE_SQL, FEATURE_STORE_TABLE_NAME, FEATURE_ENTITY_COLUMN_NAMES, FEATURE_TYPE
        FROM TILE_FEATURE_MAPPING WHERE TILE_ID ILIKE '{tile_id}' AND IS_DELETED = FALSE
    """

    df = spark.sql(select_sql)

    for row in df.collect():
        f_name = row["FEATURE_NAME"]
        f_sql = row["FEATURE_SQL"]
        fs_table = row["FEATURE_STORE_TABLE_NAME"]
        f_entity_columns = row["FEATURE_ENTITY_COLUMN_NAMES"]
        f_value_type = row["FEATURE_TYPE"]
        f_sql = f_sql.replace("__FB_POINT_IN_TIME_SQL_PLACEHOLDER", "'" + job_schedule_ts_str + "'")

        print(f_name, fs_table, f_entity_columns, f_value_type)
        print(f_sql)

        fs_table_exist = spark.catalog.tableExists(fs_table)
        print("fs_table_exist: ", fs_table_exist)

        if not fs_table_exist:
            # feature store table does not exist, create table with the input feature sql
            columns = f_entity_columns.split(",")
            columns.append(f_name)
            columns_str = ", ".join(columns)
            create_sql = f"create table {fs_table} as (select {columns_str} from ({f_sql}))"
            print(create_sql)
            spark.sql(create_sql)

        else:
            # feature store table already exists, insert records with the input feature sql
            entity_insert_cols = []
            entity_filter_cols = []
            for element in f_entity_columns.split(","):
                entity_insert_cols.append("b." + element)
                entity_filter_cols.append("a." + element + " = b." + element)

            entity_insert_cols_str = ",".join(entity_insert_cols)
            entity_filter_cols_str = " AND ".join(entity_filter_cols)

            # check whether feature value column exists, if not add the new column
            try:
                spark.sql(f"SELECT {f_name} FROM {fs_table} LIMIT 1")
            except:
                spark.sql(f"ALTER TABLE {fs_table} ADD COLUMN {f_name} {f_value_type}")

            # remove feature values for entities that are not in entity universe
            remove_values_sql = f"""
                update {fs_table} a set a.{f_name} = NULL
                    WHERE NOT EXISTS
                    (select * from ({f_sql}) b WHERE {entity_filter_cols_str})
            """
            print(remove_values_sql)
            spark.sql(remove_values_sql)

            # update or insert feature values for entities that are in entity universe
            merge_sql = f"""
                merge into {fs_table} a using ({f_sql}) b
                    on {entity_filter_cols_str}
                    when matched then
                        update set a.{f_name} = b.{f_name}
                    when not matched then
                        insert ({f_entity_columns}, {f_name})
                            values ({entity_insert_cols_str}, b.{f_name})
            """
            print(merge_sql)
            spark.sql(merge_sql)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("job_schedule_ts_str", type=str)

    args = parser.parse_args()
    main(vars(args))
