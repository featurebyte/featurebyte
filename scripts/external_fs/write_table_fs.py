"""
Write to table in external feature store
"""
from typing import Any, List

import argparse

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("FeatureStoreManagement").getOrCreate()


def check_table_exist(table_name: str) -> bool:
    """
    Check if table exists
    """
    table_exist = False
    try:
        spark.sql(f"select * from {table_name} limit 1")
        table_exist = True
    except:
        pass
    return table_exist


def get_columns(table_name: str) -> List[str]:
    """
    Get all columns of the table
    """
    result = []
    sql = f"SHOW COLUMNS IN {table_name}"
    df = spark.sql(sql)
    for row in df.collect():
        result.append(row.col_name)

    return result


def get_insert_sql(
    all_columns: List[str], primary_keys: List[str], feature_columns: List[str]
) -> str:
    """
    Get insert sql for the table
    """
    insert_sql_arr = []
    for p_col in all_columns:
        if not feature_columns or p_col in feature_columns or p_col in primary_keys:
            insert_sql_arr.append(f"b.{p_col}")
        else:
            insert_sql_arr.append("NULL")

    insert_sql = ",".join(insert_sql_arr)
    print("insert_sql: ", insert_sql)

    return insert_sql


def get_update_sql(all_columns: List[str], feature_columns: List[str]) -> str:
    """
    Get update sql for the table
    """
    columns_to_update = feature_columns
    if not feature_columns:
        columns_to_update = all_columns

    update_sql_arr = []
    for p_col in columns_to_update:
        update_sql_arr.append(f"a.{p_col} = b.{p_col}")

    update_sql = ",".join(update_sql_arr)
    print("update_sql: ", update_sql)

    return update_sql


def main(args: Any) -> None:
    """
    Write to table in external feature store
    """
    table_name = args.table_name
    feature_sql = args.feature_sql
    primary_keys_str = args.primary_keys
    feature_column_names_str = args.feature_column_names
    feature_column_types_str = args.feature_column_types

    print("table_name: ", table_name)
    print("feature_sql: ", feature_sql)
    print("primary_keys_str: ", primary_keys_str)
    print("feature_column_names_str: ", feature_column_names_str)
    print("feature_column_types_str: ", feature_column_types_str)

    primary_keys = primary_keys_str.split(",")
    print("primary_keys: ", primary_keys)

    table_exists = check_table_exist(table_name)
    print("table_exists: ", table_exists)

    if not table_exists:
        fe = FeatureEngineeringClient()

        dataframe = spark.sql(feature_sql)
        print("dataframe.schema: ", dataframe.schema)
        fe.create_table(
            name=table_name,
            primary_keys=primary_keys,
            schema=dataframe.schema,
            description="features",
        )
        dataframe.write.mode("append").saveAsTable(table_name)
    else:
        match_sql_arr = []
        for p_key in primary_keys:
            match_sql_arr.append(f"a.{p_key} = b.{p_key}")
        match_sql = " and ".join(match_sql_arr)
        print("match_sql: ", match_sql)

        all_columns = get_columns(table_name)
        print("all_columns: ", all_columns)

        feature_column_names = []
        if feature_column_names_str:
            feature_column_names = feature_column_names_str.split(",")
        print("feature_column_names: ", feature_column_names)

        feature_column_types = []
        if feature_column_types_str:
            feature_column_types = feature_column_types_str.split(",")
        print("feature_column_types: ", feature_column_types)

        # dynamically add new columns
        for col_name, col_type in zip(feature_column_names, feature_column_types):
            if col_name not in all_columns:
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({col_name} {col_type})")
                print(f"Added column {col_name} of type {col_type} to table {table_name}")
                all_columns.append(col_name)

        # merge into table
        insert_sql = get_insert_sql(all_columns, primary_keys, feature_column_names)
        update_sql = get_update_sql(all_columns, feature_column_names)

        merge_sql = f"""
            MERGE INTO {table_name} a
                USING ({feature_sql}) b
                ON {match_sql}
                WHEN MATCHED THEN
                    UPDATE SET {update_sql}
                WHEN NOT MATCHED
                    THEN INSERT ({",".join(all_columns)}) VALUES ({insert_sql})
        """

        print("merge_sql: ", merge_sql)
        spark.sql(merge_sql)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table_name", type=str)
    parser.add_argument("feature_sql", type=str)
    parser.add_argument("primary_keys", type=str)
    parser.add_argument("feature_column_names", type=str)
    parser.add_argument("feature_column_types", type=str)

    args = parser.parse_args()
    main(args)
