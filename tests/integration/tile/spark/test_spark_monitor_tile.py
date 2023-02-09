"""
Tile Monitor tests for Spark Session
"""
from datetime import datetime

from featurebyte.enum import InternalName
from featurebyte.sql.spark.tile_generate import TileGenerate
from featurebyte.sql.spark.tile_monitor import TileMonitor


def test_monitor_tile_missing_tile(spark_session):
    """
    Test monitoring with missing tiles
    """
    entity_col_names = "PRODUCT_ACTION,CUST_ID"
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 100"

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )
    tile_generate_ins.execute()

    tile_monitor_ins = TileMonitor(
        spark_session=spark_session,
        featurebyte_database="TEST_DB_1",
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )
    tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = spark_session.sql(sql).collect()
    assert len(result) == 5
    assert result[-1]["VALUE"] == 4
    assert result[-2]["VALUE"] == 11
    assert result[-3]["VALUE"] == 19

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = spark_session.sql(sql).collect()
    assert result[0].TILE_COUNT == 5
