"""
Tile Tests for Spark Session
"""
import os
from datetime import datetime

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

    test_db = "TEST_DB_1"
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {test_db}")
    spark.sql(f"USE DATABASE {test_db}")

    spark.sql(
        "CREATE OR REPLACE FUNCTION F_TIMESTAMP_TO_INDEX AS 'com.featurebyte.hive.udf.TimestampToIndex' "
        f"USING JAR 'file:///{current_dir}/udf/featurebyte-hive-udf-1.0.2-SNAPSHOT-all.jar'"
    )

    sql_table_files = [
        "T_TILE_REGISTRY.sql",
        "T_ONLINE_STORE_MAPPING.sql",
        "T_TILE_FEATURE_MAPPING.sql",
        "T_TILE_JOB_MONITOR.sql",
        "T_TILE_MONITOR_SUMMARY.sql",
    ]
    for sql_file in sql_table_files:
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


@pytest.fixture(name="tile_task_prep_spark")
def tile_task_online_store_prep(spark_session):
    entity_col_names = "__FB_TILE_START_DATE_COLUMN,PRODUCT_ACTION,CUST_ID"
    feature_name = "feature_1"
    feature_store_table_name = "fs_table_1"

    table_name = "TEMP_TABLE"
    suffix = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{suffix}"
    aggregation_id = f"some_agg_id_{suffix}"

    number_records = 2
    insert_mapping_sql = f"""
            insert into ONLINE_STORE_MAPPING(
                TILE_ID,
                AGGREGATION_ID,
                RESULT_ID,
                RESULT_TYPE,
                SQL_QUERY,
                ONLINE_STORE_TABLE_NAME,
                ENTITY_COLUMN_NAMES,
                IS_DELETED,
                CREATED_AT
            )
            values (
                '{tile_id}',
                '{aggregation_id}',
                '{feature_name}',
                'FLOAT',
                'select {entity_col_names}, cast(value_2 as float) as {feature_name} from {table_name} limit {number_records}',
                '{feature_store_table_name}',
                '{entity_col_names}',
                false,
                current_timestamp()
            )
    """
    spark_session.sql(insert_mapping_sql)

    sql = f"SELECT * FROM ONLINE_STORE_MAPPING WHERE TILE_ID = '{tile_id}'"
    result = spark_session.sql(sql).collect()
    assert len(result) == 1
    assert result[0]["TILE_ID"] == tile_id
    assert result[0]["RESULT_ID"] == feature_name

    yield tile_id, aggregation_id, feature_store_table_name, feature_name, entity_col_names

    spark_session.sql("DELETE FROM ONLINE_STORE_MAPPING")
    spark_session.sql(f"DROP TABLE IF EXISTS {feature_store_table_name}")
