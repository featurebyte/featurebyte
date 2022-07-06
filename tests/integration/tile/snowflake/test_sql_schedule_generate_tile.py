"""
This module contains integration tests for scheduled tile generation stored procedure
"""
from datetime import datetime

from featurebyte.enum import InternalName


def test_schedule_generate_tile_online(snowflake_session):
    """
    Test the stored procedure of generating tiles
    """
    col_names = f"{InternalName.TILE_START_DATE},PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:58:00Z"
    tile_sql = (
        f" SELECT {col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    sql = f"call SP_TILE_GENERATE_SCHEDULE('{tile_id}', 183, 3, 5, 1440, '{tile_sql}', '{InternalName.TILE_START_DATE}', '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}', '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}', '{col_names}', 'ONLINE', {tile_monitor}, '{tile_end_ts}')"
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}"
    result = snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == (tile_monitor + 1)


def test_schedule_monitor_tile_online(snowflake_session):
    """
    Test the stored procedure of monitoring tiles
    """
    col_names = f"{InternalName.TILE_START_DATE},PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"TEMP_TABLE_{datetime.now().strftime('%Y%m%d%H%M%S_%f')}"
    tile_monitor = 10
    tile_end_ts = "2022-06-05T23:53:00Z"
    tile_sql = (
        f" SELECT {col_names} FROM {table_name} "
        f" WHERE {InternalName.TILE_START_DATE} >= {InternalName.TILE_START_DATE_SQL_PLACEHOLDER} "
        f" AND {InternalName.TILE_START_DATE} < {InternalName.TILE_END_DATE_SQL_PLACEHOLDER}"
    )

    sql = (
        f"call SP_TILE_GENERATE_SCHEDULE("
        f"  '{tile_id}',"
        f"  183,"
        f"  3,"
        f"  5,"
        f"  1440,"
        f"  '{tile_sql}',"
        f"  '{InternalName.TILE_START_DATE}',"
        f"  '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',"
        f"  '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',"
        f"  '{col_names}',"
        f"  'ONLINE',"
        f"  {tile_monitor},"
        f"  '{tile_end_ts}'"
        f")"
    )
    result = snowflake_session.execute_query(sql)
    assert "Debug" in result["SP_TILE_GENERATE_SCHEDULE"].iloc[0]

    sql = f"UPDATE {table_name} SET VALUE = VALUE + 1 WHERE {InternalName.TILE_START_DATE} in ('2022-06-05T23:48:00Z', '2022-06-05T23:33:00Z') "
    snowflake_session.execute_query(sql)

    tile_end_ts_2 = "2022-06-05T23:58:00Z"
    sql = (
        f"call SP_TILE_GENERATE_SCHEDULE("
        f"  '{tile_id}',"
        f"  183,"
        f"  3,"
        f"  5,"
        f"  1440,"
        f"  '{tile_sql}',"
        f"  '{InternalName.TILE_START_DATE}',"
        f"  '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}',"
        f"  '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}',"
        f"  '{col_names}',"
        f"  'ONLINE',"
        f"  {tile_monitor},"
        f"  '{tile_end_ts_2}'"
        f")"
    )
    snowflake_session.execute_query(sql)

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_id}_MONITOR"
    result = snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 2
