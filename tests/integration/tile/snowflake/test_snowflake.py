"""
This module contains integration tests for TileSnowflake
"""


def test_generate_tile(snowflake_tile, fb_db_session):
    """
    Test generate_tiles method in TileSnowflake
    """
    snowflake_tile.generate_tiles("ONLINE", "2022-06-05 23:33:00", "2022-06-05 23:58:00")

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = fb_db_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


def test_schedule_online_tile(snowflake_tile, fb_db_session):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    snowflake_tile.schedule_online_tiles(start_task=False)

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_ONLINE"

    result = fb_db_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3-59/5 * * * * UTC"


def test_schedule_offline_tile(snowflake_tile, fb_db_session):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    snowflake_tile.schedule_offline_tiles(start_task=False)

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_OFFLINE"

    result = fb_db_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
