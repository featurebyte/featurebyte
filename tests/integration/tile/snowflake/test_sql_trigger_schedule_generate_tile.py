"""
This module contains integration tests for scheduled tile generation stored procedure
"""

from featurebyte.enum import InternalName


def test_trigger_tile_schedule(snowflake_session):
    """
    Test creation of scheduled task for tile generation and monitoring
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_id = f"{table_name}_TILE"
    tile_monitor = 10
    tile_sql = (
        f" SELECT {col_names} FROM {table_name} "
        f" WHERE TILE_START_TS >= FB_START_TS "
        f" AND TILE_START_TS < FB_END_TS"
    )
    task_name = f"TILE_TASK_ONLINE_{tile_id}"

    internal_names = f"'{InternalName.TILE_START_DATE}', '{InternalName.TILE_START_DATE_SQL_PLACEHOLDER}', '{InternalName.TILE_END_DATE_SQL_PLACEHOLDER}'"
    sql = (
        f"call SP_TILE_TRIGGER_GENERATE_SCHEDULE(null, 'COMPUTE_WH', '{tile_id}', 181, 1, 5, 1440, "
        f"'{tile_sql}', {internal_names}, '{col_names}', 'ONLINE', {tile_monitor})"
    )
    snowflake_session.execute_query(sql)

    result = snowflake_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "5 MINUTE"

    snowflake_session.execute_query(f"DROP TASK IF EXISTS {task_name}")
    result = snowflake_session.execute_query("SHOW TASKS")
    assert len(result) == 0
