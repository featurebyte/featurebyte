"""
Tile Monitor tests for Spark and Snowflake Session
"""
from datetime import datetime

import pytest

from featurebyte.sql.common import construct_create_table_query
from featurebyte.sql.tile_generate import TileGenerate
from featurebyte.sql.tile_monitor import TileMonitor


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile__missing_tile(session, base_sql_model):
    """
    Test monitoring with missing tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 95"
    )
    monitor_tile_sql = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 100"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_generate_ins.execute()

    tile_monitor_ins = TileMonitor(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert len(result) == 5
    assert result["VALUE"].iloc[-1] == 4
    assert result["VALUE"].iloc[-2] == 11
    assert result["VALUE"].iloc[-3] == 19

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile__updated_tile(session, base_sql_model):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = f"SOURCE_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    create_sql = construct_create_table_query(
        table_name, "select * from TEMP_TABLE", session=session
    )
    await session.execute_query(create_sql)

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 10"
    )
    monitor_tile_sql = tile_sql

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_generate_ins.execute()

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    await session.execute_query(sql)

    tile_monitor_ins = TileMonitor(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert len(result) == 10
    assert result["VALUE"].iloc[0] == 6
    assert result["OLD_VALUE"].iloc[0] == 5
    assert result["VALUE"].iloc[1] == 3
    assert result["OLD_VALUE"].iloc[1] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 10


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile__updated_tile_new_column(session, base_sql_model):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = f"SOURCE_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    create_sql = construct_create_table_query(
        table_name, "select * from TEMP_TABLE", session=session
    )
    await session.execute_query(create_sql)

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 10"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_generate_ins.execute()

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    await session.execute_query(sql)

    value_col_names_2 = ["VALUE", "VALUE_2"]
    value_col_types_2 = ["FLOAT", "FLOAT"]
    value_col_names_2_str = ",".join(value_col_names_2)
    monitor_tile_sql_2 = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_2_str} FROM {table_name} limit 10"
    )

    tile_monitor_ins = TileMonitor(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql_2,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names_2,
        value_column_types=value_col_types_2,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert len(result) == 10

    assert result["VALUE"].iloc[0] is not None
    assert result["OLD_VALUE"].iloc[0] is not None

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 10


@pytest.mark.parametrize("source_type", ["spark", "snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile__partial_columns(session, base_sql_model):
    """
    Test monitoring with missing tiles
    """
    entity_col_names = ["PRODUCT_ACTION", "CUST_ID", "客户"]
    value_col_names = ["VALUE"]
    value_col_types = ["FLOAT"]
    table_name = "TEMP_TABLE"
    ts_str = datetime.now().strftime("%Y%m%d%H%M%S_%f")
    tile_id = f"TEMP_TABLE_{ts_str}"
    agg_id = f"AGG_ID_{ts_str}"

    entity_col_names_str = ",".join([base_sql_model.quote_column(col) for col in entity_col_names])
    value_col_names_str = ",".join(value_col_names)
    tile_sql = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 90"
    )
    monitor_tile_sql = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 95"
    )

    tile_generate_ins = TileGenerate(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_generate_ins.execute()

    tile_monitor_ins = TileMonitor(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_monitor_ins.execute()

    monitor_tile_sql2 = (
        f"SELECT INDEX,{entity_col_names_str},{value_col_names_str} FROM {table_name} limit 100"
    )
    await session.execute_query(f"ALTER TABLE {tile_id}_MONITOR ADD COLUMN VALUE1 FLOAT")

    tile_monitor_ins = TileMonitor(
        session=session,
        tile_id=tile_id,
        time_modulo_frequency_second=183,
        blind_spot_second=3,
        frequency_minute=5,
        sql=tile_sql,
        monitor_sql=monitor_tile_sql2,
        entity_column_names=entity_col_names,
        value_column_names=value_col_names,
        value_column_types=value_col_types,
        tile_type="ONLINE",
        aggregation_id=agg_id,
    )
    await tile_monitor_ins.execute()

    sql = f"SELECT * FROM {tile_id}_MONITOR ORDER BY CREATED_AT DESC"
    result = await session.execute_query(sql)
    assert len(result) == 15
    assert result["VALUE"].iloc[-1] == 11
    assert result["VALUE"].iloc[-2] == 13
    assert result["VALUE"].iloc[-3] == 8

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 15
