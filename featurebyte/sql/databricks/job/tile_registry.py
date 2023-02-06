"""
Databricks Tile Registry Job Script
"""
from typing import Any, Dict

import argparse

from pyspark.sql import SparkSession


def main(args: Dict[str, Any]) -> None:

    spark = SparkSession.builder.appName("TileManagement").getOrCreate()

    featurebyte_database = args["featurebyte_database"]
    sql = args["sql"]
    tile_modulo_frequency_second = args["tile_modulo_frequency_second"]
    blind_spot_second = args["blind_spot_second"]
    frequency_minute = args["frequency_minute"]
    entity_column_names = args["entity_column_names"]
    value_column_names = args["value_column_names"]
    value_column_types = args["value_column_types"]
    tile_id = args["tile_id"].upper()
    table_name = args["table_name"]
    table_exist = args["table_exist"]

    print("featurebyte_database: ", featurebyte_database)
    print("sql: ", sql)
    print("tile_modulo_frequency_second: ", tile_modulo_frequency_second)
    print("blind_spot_second: ", blind_spot_second)
    print("frequency_minute: ", frequency_minute)
    print("entity_column_names: ", entity_column_names)
    print("value_column_names: ", value_column_names)
    print("value_column_types: ", value_column_types)
    print("tile_id: ", tile_id)
    print("table_name: ", table_name)
    print("table_exist: ", table_exist)

    spark.sql(f"USE DATABASE {featurebyte_database}")

    df = spark.sql(
        f"select VALUE_COLUMN_NAMES as names, VALUE_COLUMN_TYPES as types from tile_registry where tile_id = '{tile_id}'"
    )

    res = df.select("names", "types").collect()
    print("res: ", res)

    input_value_columns = [value for value in value_column_names.split(",") if value.strip()]
    print("input_value_columns: ", input_value_columns)

    input_value_columns_types = [value for value in value_column_types.split(",") if value.strip()]
    print("input_value_columns_types: ", input_value_columns_types)

    if res:
        value_cols = res[0].names
        value_cols_types = res[0].types
        print("value_cols: ", value_cols)
        print("value_cols_types: ", value_cols_types)

        exist_columns = [value for value in value_cols.split(",") if value.strip()]
        exist_columns_types = [value for value in value_cols_types.split(",") if value.strip()]

        for i, input_column in enumerate(input_value_columns):
            if input_column not in exist_columns:
                exist_columns.append(input_column)

            input_column_type = input_value_columns_types[i]
            if input_column_type not in exist_columns_types:
                exist_columns_types.append(input_column_type)

        new_value_columns_str = ",".join(exist_columns)
        print("new_value_columns_str: ", new_value_columns_str)
        new_value_columns_types_str = ",".join(exist_columns_types)
        print("new_value_columns_types_str: ", new_value_columns_types_str)

        update_sql = f"""
                        UPDATE TILE_REGISTRY SET
                            VALUE_COLUMN_NAMES = '{new_value_columns_str}',
                            VALUE_COLUMN_TYPES = '{new_value_columns_types_str}'
                        WHERE TILE_ID = '{tile_id}'
                     """
        spark.sql(update_sql)
    else:
        print("No value columns")
        escape_sql = sql.replace("'", "''")
        insert_sql = f"""
            insert into tile_registry(
                TILE_ID, TILE_SQL, ENTITY_COLUMN_NAMES, VALUE_COLUMN_NAMES, VALUE_COLUMN_TYPES, FREQUENCY_MINUTE, TIME_MODULO_FREQUENCY_SECOND,
                BLIND_SPOT_SECOND, IS_ENABLED, CREATED_AT,
                LAST_TILE_START_DATE_ONLINE, LAST_TILE_INDEX_ONLINE, LAST_TILE_START_DATE_OFFLINE, LAST_TILE_INDEX_OFFLINE
            )
            VALUES (
                '{tile_id}', '{escape_sql}', '{entity_column_names}', '{value_column_names}', '{value_column_types}', {frequency_minute}, {tile_modulo_frequency_second},
                {blind_spot_second}, TRUE, current_timestamp(),
                null, null, null, null
            )
        """
        print("insert_sql: ", insert_sql)
        spark.sql(insert_sql)

    if table_exist == "Y":
        df = spark.sql(f"SHOW COLUMNS IN {table_name}")
        cols = []
        for col in df.collect():
            cols.append(col.col_name)

        print("cols: ", cols)

        tile_add_sql = f"ALTER TABLE {table_name} ADD COLUMN\n"
        add_statements = []
        for i, input_column in enumerate(input_value_columns):
            if input_column not in cols:
                element_type = input_value_columns_types[i]
                add_statements.append(f"{input_column} {element_type} DEFAULT NULL")
                if "_MONITOR" in table_name:
                    add_statements.append(f"OLD_{input_column} {element_type} DEFAULT NULL")

        if add_statements:
            tile_add_sql += ",\n".join(add_statements)
            print("tile_add_sql: ", tile_add_sql)
            spark.sql(tile_add_sql)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("featurebyte_database", type=str)
    parser.add_argument("sql", type=str)
    parser.add_argument("tile_modulo_frequency_second", type=int)
    parser.add_argument("blind_spot_second", type=int)
    parser.add_argument("frequency_minute", type=int)
    parser.add_argument("entity_column_names", type=str)
    parser.add_argument("value_column_names", type=str)
    parser.add_argument("value_column_types", type=str)
    parser.add_argument("tile_id", type=str)
    parser.add_argument("table_name", type=str)
    parser.add_argument("table_exist", type=str)

    args = parser.parse_args()
    main(vars(args))
