"""
Databricks Tile Monitor Job Script
"""
from typing import Any, Dict

import argparse

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("TileManagement").getOrCreate()
spark.sparkContext.addPyFile("dbfs:/FileStore/newudfs/tile_registry.py")

import tile_registry


def main(args: Dict[str, Any]) -> None:

    featurebyte_database = args["featurebyte_database"]
    monitor_sql = args["monitor_sql"]
    tile_start_date_column = args["tile_start_date_column"]
    tile_modulo_frequency_second = args["tile_modulo_frequency_second"]
    blind_spot_second = args["blind_spot_second"]
    frequency_minute = args["frequency_minute"]
    entity_column_names = args["entity_column_names"]
    value_column_names = args["value_column_names"]
    tile_id = args["tile_id"].upper()
    tile_type = args["tile_type"]

    print("featurebyte_database: ", featurebyte_database)
    print("tile_start_date_column: ", tile_start_date_column)
    print("tile_modulo_frequency_second: ", tile_modulo_frequency_second)
    print("blind_spot_second: ", blind_spot_second)
    print("frequency_minute: ", frequency_minute)
    print("entity_column_names: ", entity_column_names)
    print("value_column_names: ", value_column_names)
    print("tile_id: ", tile_id)
    print("tile_type: ", tile_type)

    spark.sql(f"USE DATABASE {featurebyte_database}")

    tile_table_exist = spark.catalog.tableExists(tile_id)
    print("tile_table_exist: ", tile_table_exist)

    if not tile_table_exist:
        print(f"tile table {tile_id} does not exist")
    else:
        df = spark.sql(f"select value_column_names from tile_registry where tile_id = '{tile_id}'")
        existing_value_columns = df.collect()[0].value_column_names

        tile_sql = monitor_sql.replace("'", "''")
        args["sql"] = tile_sql
        args["table_name"] = tile_id
        args["table_exist"] = "Y"

        print("\n\nCalling tile_registry.main\n")
        tile_registry.main(args)
        print("\nEnd of calling tile_registry.main\n\n")

        new_tile_sql = f"""
            select
                {tile_start_date_column},
                F_TIMESTAMP_TO_INDEX({tile_start_date_column}, {tile_modulo_frequency_second}, {blind_spot_second}, {frequency_minute}) as INDEX,
                {entity_column_names},
                {existing_value_columns}
            from ({monitor_sql})
        """
        print("new_tile_sql: ", new_tile_sql)

        entity_filter_cols_str = " AND ".join(
            [f"a.{c} = b.{c}" for c in entity_column_names.split(",")]
        )
        value_select_cols_str = " , ".join(
            [f"b.{c} as old_{c}" for c in existing_value_columns.split(",")]
        )
        value_insert_cols_str = " , ".join([f"old_{c}" for c in existing_value_columns.split(",")])
        value_filter_cols_str = " OR ".join(
            [
                f"{c} != old_{c} or ({c} is not null and old_{c} is null)"
                for c in existing_value_columns.split(",")
            ]
        )

        compare_sql = f"""
            select * from
                (select
                    a.*,
                    {value_select_cols_str},
                    cast('{tile_type}' as string) as TILE_TYPE,
                    DATEADD(SECOND, ({blind_spot_second}+{frequency_minute}*60), a.{tile_start_date_column}) as EXPECTED_CREATED_AT,
                    current_timestamp() as CREATED_AT
                from
                    ({new_tile_sql}) a left outer join {tile_id} b
                on
                    a.INDEX = b.INDEX AND {entity_filter_cols_str})
            where {value_filter_cols_str}
        """
        print("compare_sql: ", compare_sql)

        monitor_table_name = f"{tile_id}_MONITOR"

        tile_monitor_exist = spark.catalog.tableExists(monitor_table_name)
        print("tile_monitor_exist: ", tile_monitor_exist)

        if not tile_monitor_exist:
            spark.sql(f"create table {monitor_table_name} as {compare_sql}")
        else:
            args["table_name"] = monitor_table_name
            args["table_exist"] = "Y"

            print("\n\nCalling tile_registry.main\n")
            tile_registry.main(args)
            print("\nEnd of calling tile_registry.main\n\n")

            insert_sql = f"""
                insert into {monitor_table_name}
                    ({tile_start_date_column}, INDEX, {entity_column_names}, {existing_value_columns}, {value_insert_cols_str}, TILE_TYPE, EXPECTED_CREATED_AT, CREATED_AT)
                    {compare_sql}
            """
            print(insert_sql)
            spark.sql(insert_sql)

            insert_monitor_summary_sql = f"""
                INSERT INTO TILE_MONITOR_SUMMARY(TILE_ID, TILE_START_DATE, TILE_TYPE, CREATED_AT)
                SELECT
                    '{tile_id}' as TILE_ID,
                    {tile_start_date_column} as TILE_START_DATE,
                    TILE_TYPE,
                    current_timestamp()
                FROM ({compare_sql})
            """
            print(insert_monitor_summary_sql)
            spark.sql(insert_monitor_summary_sql)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("monitor_sql", type=str)
    parser.add_argument("tile_start_date_column", type=str)
    parser.add_argument("tile_modulo_frequency_second", type=int)
    parser.add_argument("blind_spot_second", type=int)
    parser.add_argument("frequency_minute", type=int)
    parser.add_argument("entity_column_names", type=str)
    parser.add_argument("value_column_names", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("tile_type", type=str)

    args = parser.parse_args()
    main(vars(args))
