"""
Tile Monitor tests for Spark Session
"""
from datetime import datetime

import pytest

from featurebyte.enum import InternalName
from featurebyte.sql.spark.tile_generate import TileGenerate
from featurebyte.sql.spark.tile_monitor import TileMonitor


@pytest.mark.asyncio
async def test_monitor_tile_missing_tile(spark_session):
    """
    Test monitoring with missing tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "`客户`"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    entity_col_names_str = ",".join(entity_col_names)
    value_col_names_str = ",".join(value_col_names)
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 100"

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
    await tile_generate_ins.execute()

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
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await spark_session.execute_query(sql)
    assert len(result) == 5
    assert result["VALUE"].iloc[-1] == 4
    assert result["VALUE"].iloc[-2] == 11
    assert result["VALUE"].iloc[-3] == 19

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await spark_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.asyncio
async def test_monitor_tile_updated_tile(spark_session):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "`客户`"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = f"SOURCE_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    await spark_session.execute_query(
        f"create table {table_name} using delta as select * from TEMP_TABLE"
    )

    entity_col_names_str = ",".join(entity_col_names)
    value_col_names_str = ",".join(value_col_names)
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 10"
    monitor_tile_sql = tile_sql

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
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
    await tile_generate_ins.execute()

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    await spark_session.execute_query(sql)

    tile_monitor_ins = TileMonitor(
        spark_session=spark_session,
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
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await spark_session.execute_query(sql)
    assert len(result) == 10
    assert result["VALUE"].iloc[0] == 6
    assert result["OLD_VALUE"].iloc[0] == 5
    assert result["VALUE"].iloc[1] == 3
    assert result["OLD_VALUE"].iloc[1] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await spark_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 10


@pytest.mark.asyncio
async def test_monitor_tile_updated_tile_new_column(spark_session):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "`客户`"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = f"SOURCE_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"

    await spark_session.execute_query(
        f"create table {table_name} using delta as select * from TEMP_TABLE"
    )

    entity_col_names_str = ",".join(entity_col_names)
    value_col_names_str = ",".join(value_col_names)
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 10"

    tile_generate_ins = TileGenerate(
        spark_session=spark_session,
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
    await tile_generate_ins.execute()

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    await spark_session.execute_query(sql)

    value_col_names_2 = ["VALUE", "VALUE_2"]
    value_col_types_2 = ["FLOAT", "FLOAT"]
    value_col_names_2_str = ",".join(value_col_names_2)
    monitor_tile_sql_2 = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names_str},{value_col_names_2_str} FROM {table_name} limit 10"

    tile_monitor_ins = TileMonitor(
        spark_session=spark_session,
        tile_id=tile_id,
        tile_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql_2,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names_2,
        value_column_types=value_col_types_2,
        tile_type="ONLINE",
        tile_start_date_column=InternalName.TILE_START_DATE,
    )
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await spark_session.execute_query(sql)
    assert len(result) == 10

    assert result["VALUE"].iloc[0] is not None
    assert result["OLD_VALUE"].iloc[0] is not None

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await spark_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 10
