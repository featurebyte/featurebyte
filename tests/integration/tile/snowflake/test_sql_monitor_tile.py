"""
This module contains integration tests for tile monitoring stored procedure
"""
from datetime import datetime

from featurebyte.enum import InternalName


def test_monitor_tile_missing_tile(snowflake_session):
    """
    Test monitoring with missing tiles
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 95"
    monitor_tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 100"

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, '{entity_col_names}', "
        f"'{value_col_names}', '{tile_id}', 'ONLINE', null)"
    )
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, "
        f"'{entity_col_names}', '{value_col_names}', '{tile_id}', 'ONLINE')"
    )
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]
    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 5

    assert result.iloc[-1]["VALUE"] == 4
    assert result.iloc[-1]["OLD_VALUE"] is None
    assert result.iloc[-2]["VALUE"] == 11
    assert result.iloc[-2]["OLD_VALUE"] is None
    assert result.iloc[-3]["VALUE"] == 19
    assert result.iloc[-3]["OLD_VALUE"] is None
    assert result.iloc[-3]["客户"] == 1


def test_monitor_tile_updated_tile(snowflake_session):
    """
    Test monitoring with outdated tiles in which the tile value has been incremented by 1
    """
    entity_col_names = 'PRODUCT_ACTION,CUST_ID,"客户"'
    value_col_names = "VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_sql = f"SELECT {InternalName.TILE_START_DATE},{entity_col_names},{value_col_names} FROM {table_name} limit 10"
    monitor_tile_sql = tile_sql

    sql = (
        f"call SP_TILE_GENERATE('{tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, '{entity_col_names}', "
        f"'{value_col_names}', '{tile_id}', 'ONLINE', null)"
    )
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    snowflake_session.execute_query(sql)

    sql = (
        f"call SP_TILE_MONITOR('{monitor_tile_sql}', '{InternalName.TILE_START_DATE}', 183, 3, 5, '{entity_col_names}', "
        f"'{value_col_names}', '{tile_id}', 'ONLINE')"
    )
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]

    sql = f"SELECT * FROM {tile_id}_MONITOR"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 10

    assert result.iloc[0]["VALUE"] == 6
    assert result.iloc[0]["OLD_VALUE"] == 5
    assert result.iloc[1]["VALUE"] == 3
    assert result.iloc[1]["OLD_VALUE"] == 2
