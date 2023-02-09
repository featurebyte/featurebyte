"""
Tile Tests for Spark Session
"""
import os

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


@pytest.fixture(name="spark_session", scope="session")
def spark_session_fixture():
    """
    Spark session
    """
    current_dir = os.path.dirname(__file__)

    script_path = os.path.join(current_dir, "..", "..", "..", "..", "featurebyte", "sql", "spark")

    spark = (
        SparkSession.builder.appName("TileManagement")
        .config(
            "spark.jars",
            f"{current_dir}/jars/delta-core_2.12-2.2.0.jar,{current_dir}/jars/delta-storage-2.2.0.jar",
        )
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    py_files = [
        "tile_common.py",
        "tile_registry.py",
        "tile_monitor.py",
        "tile_generate.py",
        "tile_generate_entity_tracking.py",
        "tile_schedule_online_store.py",
    ]
    for py_file in py_files:
        spark.sparkContext.addPyFile(f"{script_path}/{py_file}")

    test_db = "TEST_DB_1"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    spark.sql(f"USE DATABASE {test_db}")

    spark.sql(
        "CREATE OR REPLACE FUNCTION F_TIMESTAMP_TO_INDEX AS 'com.featurebyte.hive.udf.TimestampToIndex' "
        f"USING JAR 'file:///{current_dir}/udf/featurebyte-hive-udf-1.0.2-SNAPSHOT-all.jar'"
    )

    sql_table_files = [
        ("T_TILE_REGISTRY.sql", "TILE_REGISTRY"),
        ("T_ONLINE_STORE_MAPPING.sql", "ONLINE_STORE_MAPPING"),
        ("T_TILE_FEATURE_MAPPING.sql", "TILE_FEATURE_MAPPING"),
        ("T_TILE_JOB_MONITOR.sql", "TILE_JOB_MONITOR"),
        ("T_TILE_MONITOR_SUMMARY.sql", "TILE_MONITOR_SUMMARY"),
    ]
    for sql_file, sql_table in sql_table_files:
        with open(f"{script_path}/{sql_file}", "r") as file:
            spark.sql(file.read())

    table_name = "TEMP_TABLE"
    file_path = os.path.join(current_dir, "..", "tile_data.csv")
    df = spark.read.csv(file_path, header=True)

    # 2022-06-05T23:58:00Z --> "yyyy-MM-ddTHH:mm:ssZ"
    df2 = df.withColumn(
        "__FB_TILE_START_DATE_COLUMN",
        to_timestamp(col("__FB_TILE_START_DATE_COLUMN"), "yyyy-MM-dd'T'HH:mm:ss'Z'"),
    )

    df2 = df2.withColumn(
        "VALUE",
        col("VALUE").cast("int"),
    )
    df2 = df2.withColumn(
        "VALUE_2",
        col("VALUE_2").cast("int"),
    )
    df2 = df2.withColumn(
        "VALUE_3",
        col("VALUE_3").cast("int"),
    )

    df2.write.format("delta").saveAsTable(name=table_name, mode="overwrite")

    yield spark

    spark.sql(f"DROP DATABASE IF EXISTS {test_db} CASCADE")

    spark.stop()
