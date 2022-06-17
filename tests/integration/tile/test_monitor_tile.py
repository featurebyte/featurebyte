"""
This module contains integration tests for tile monitoring stored procedure
"""


def test_monitor_tile_missing_tile(fb_db_session):
    """
    Test monitoring with missing tiles
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {col_names} FROM {table_name} limit 90"
    monitor_tile_sql = f"SELECT {col_names} FROM {table_name} limit 95"

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"call SP_TILE_MONITOR('{monitor_tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE', 'ONLINE')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]

    sql = f"SELECT COUNT(*) as MONITOR_COUNT FROM {table_name}_TILE_MONITOR"
    result = fb_db_session.execute_query(sql)
    assert result["MONITOR_COUNT"].iloc[0] == 5


def test_monitor_tile_updated_tile(fb_db_session):
    """
    Test monitoring with outdated tiles
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_sql = f"SELECT {col_names} FROM {table_name} limit 10"
    monitor_tile_sql = tile_sql

    sql = f"call SP_TILE_GENERATE('{tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1"
    fb_db_session.execute_query(sql)

    sql = f"call SP_TILE_MONITOR('{monitor_tile_sql}', 183, 3, 5, '{col_names}', '{table_name}_TILE', 'ONLINE')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_MONITOR"].iloc[0]

    sql = f"SELECT COUNT(*) as MONITOR_COUNT FROM {table_name}_TILE_MONITOR"
    result = fb_db_session.execute_query(sql)
    assert result["MONITOR_COUNT"].iloc[0] == 10
