"""
This module contains integration tests for scheduled tile generation stored procedure
"""
from datetime import datetime


def test_schedule_generate_tile_online(fb_db_session):
    """
    Test the stored procedure of generating tiles
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:58:00Z"
    tile_sql = (
        f" SELECT {col_names} FROM {table_name} "
        f" WHERE TILE_START_TS >= FB_START_TS "
        f" AND TILE_START_TS < FB_END_TS"
    )

    sql = f"call SP_TILE_GENERATE_SCHEDULE('{tile_id}', 183, 3, 5, 1440, '{tile_sql}', '{col_names}', 'ONLINE', {tile_monitor}, '{tile_end_ts}')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = fb_db_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == (tile_monitor + 1)


def test_schedule_monitor_tile_online(fb_db_session):
    """
    Test the stored procedure of monitoring tiles
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:53:00Z"
    tile_sql = (
        f" SELECT {col_names} FROM {table_name} "
        f" WHERE TILE_START_TS >= FB_START_TS "
        f" AND TILE_START_TS < FB_END_TS"
    )

    sql = f"call SP_TILE_GENERATE_SCHEDULE('{tile_id}', 183, 3, 5, 1440, '{tile_sql}', '{col_names}', 'ONLINE', {tile_monitor}, '{tile_end_ts}')"
    result = fb_db_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1 WHERE TILE_START_TS in ('2022-06-05T23:48:00Z', '2022-06-05T23:33:00Z') "
    fb_db_session.execute_query(sql)

    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    sql = f"call SP_TILE_GENERATE_SCHEDULE('{tile_id}', 183, 3, 5, 1440, '{tile_sql}', '{col_names}', 'ONLINE', {tile_monitor}, '{tile_end_ts_2}')"
    fb_db_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = fb_db_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2
