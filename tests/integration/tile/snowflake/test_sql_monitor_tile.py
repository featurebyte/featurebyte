"""
This module contains integration tests for tile monitoring stored procedure
"""
from datetime import datetime

import pandas as pd
import pytest

from featurebyte.enum import InternalName


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile_missing_tile(session):
    """
    Test monitoring with missing tiles
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 100"

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE', null)"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, "
        f"'{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE')"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]
    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert len(result) == 5

    assert result.iloc[-1]["VALUE"] == 4
    assert pd.isna(result.iloc[-1]["OLD_VALUE"])
    assert result.iloc[-2]["VALUE"] == 11
    assert pd.isna(result.iloc[-2]["OLD_VALUE"])
    assert result.iloc[-3]["VALUE"] == 19
    assert pd.isna(result.iloc[-3]["OLD_VALUE"])
    assert result.iloc[-3]["客户"] == 1

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile_updated_tile(session):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 10"
    monitor_tile_sql = tile_sql

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE', null)"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    await session.execute_query(sql)

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, '{entity_col_names}', "
        f"'{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE')"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert len(result) == 10

    assert result.iloc[0]["VALUE"] == 6
    assert result.iloc[0]["OLD_VALUE"] == 5
    assert result.iloc[1]["VALUE"] == 3
    assert result.iloc[1]["OLD_VALUE"] == 2

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 10


@pytest.mark.parametrize("source_type", ["snowflake"], indirect=True)
@pytest.mark.asyncio
async def test_monitor_tile_updated_tile_new_column(session):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    value_col_types = "FLOAT"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 10"

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_LAST_START_DATE}', "
        f"183, 3, 5, '{entity_col_names}', '{value_col_names}', '{value_col_types}', '{tile_id}', 'ONLINE', null)"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    await session.execute_query(sql)

    value_col_names_2 = "VALUE,VALUE_2"
    value_col_types_2 = "FLOAT,FLOAT"
    monitor_tile_sql_2 = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names_2} FROM {table_name} limit 10"
    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql_2}', '{InternalName.TILE_START_DATE}', 183, 3, 5, '{entity_col_names}', "
        f"'{value_col_names_2}', '{value_col_types_2}', '{tile_id}', 'ONLINE')"
    )
    result = await session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = await session.execute_query(sql)
    assert len(result) == 10

    assert pd.notna(result.iloc[0]["VALUE"])
    assert pd.notna(result.iloc[0]["OLD_VALUE"])

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM TILE_MONITOR_SUMMARY WHERE TILE_ID = '{tile_id}'"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 10
