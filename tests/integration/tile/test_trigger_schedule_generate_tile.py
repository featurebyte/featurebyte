"""
This module contains integration tests for scheduled tile generation stored procedure
"""


def test_trigger_tile_schedule(fb_db_session):
    """
    Test creation of scheduled task for tile generation and monitoring
    """
    col_names = "TILE_START_TS,PRODUCT_ACTION,CUST_ID,VALUE"
    table_name = "TEMP_TABLE"
    tile_monitor = 10
    tile_sql = (
        f" SELECT {col_names} FROM {table_name} "
        f" WHERE TILE_START_TS >= FB_START_TS "
        f" AND TILE_START_TS < FB_END_TS"
    )
    task_name = f"SP_TILE_GENERATE_SCHEDULE_TASK_{table_name}"

    sql = (
        f"call SP_TILE_TRIGGER_GENERATE_SCHEDULE(null, 'COMPUTE_WH', '{table_name}', 181, 1, 5, 1440, "
        f"'{tile_sql}', '{col_names}', 'ONLINE', {tile_monitor})"
    )
    fb_db_session.execute_query(sql)

    result = fb_db_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "5 MINUTE"

    fb_db_session.execute_query(f"DROP TASK IF EXISTS {task_name}")
    result = fb_db_session.execute_query("SHOW TASKS")
    assert len(result) == 0
