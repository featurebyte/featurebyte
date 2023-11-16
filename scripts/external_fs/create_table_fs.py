"""
Create table in external feature store
"""

from typing import Any, List

import argparse

from databricks.feature_engineering import FeatureEngineeringClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    ByteType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


def get_spark_struct_type(column_names: List[str], column_types: List[str]) -> StructType:
    """
    Get spark struct type from column names and column types
    """
    canonical_to_spark_types = {
        "string": StringType,
        "int": IntegerType,
        "integer": IntegerType,
        "boolean": BooleanType,
        "tinyint": ByteType,
        "smallint": ShortType,
        "bigint": LongType,
        "float": FloatType,
        "double": DoubleType,
        "decimal": DecimalType,
        "date": DateType,
        "timestamp": TimestampType,
    }

    spark_feature_column_types = [canonical_to_spark_types.get(x, StringType) for x in column_types]

    type_arr = []
    for column_name, column_type in zip(column_names, spark_feature_column_types):
        type_arr.append(StructField(column_name, column_type(), True))

    return StructType(type_arr)


def main(args: Any) -> None:
    """
    Create table in external feature store
    """
    spark = SparkSession.builder.appName("FeatureStoreManagement").getOrCreate()

    table_name = args.table_name
    primary_keys_str = args.primary_keys
    timestamp_keys_str = args.timestamp_keys
    column_names_str = args.column_names
    column_types_str = args.column_types
    feature_sql = args.feature_sql

    print("table_name: ", table_name)
    print("column_names_str: ", column_names_str)
    print("column_types_str: ", column_types_str)
    print("feature_sql: ", feature_sql)
    print("primary_keys_str: ", primary_keys_str)
    print("timestamp_keys_str: ", timestamp_keys_str)

    primary_keys = primary_keys_str.split(",")
    print("primary_keys: ", primary_keys)

    dataframe = None
    if column_types_str:
        all_columns = column_names_str.split(",")
        schema = get_spark_struct_type(all_columns, column_types_str.split(","))
    else:
        dataframe = spark.sql(feature_sql)
        schema = dataframe.schema

    timestamp_keys = None
    if timestamp_keys_str:
        timestamp_keys = timestamp_keys_str.split(",")
    print("timestamp_keys: ", timestamp_keys)

    fe = FeatureEngineeringClient()
    fe.create_table(
        name=table_name,
        primary_keys=primary_keys,
        timeseries_columns=timestamp_keys,
        schema=schema,
        description="features",
    )

    if feature_sql and dataframe:
        dataframe.write.mode("append").saveAsTable(table_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table_name", type=str)
    parser.add_argument("primary_keys", type=str)
    parser.add_argument("timestamp_keys", type=str)
    parser.add_argument("column_names", type=str)
    parser.add_argument("column_types", type=str)
    parser.add_argument("feature_sql", type=str)

    args = parser.parse_args()
    main(args)
