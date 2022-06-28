"""
This module contains integration tests for TileSnowflake
"""


def test_generate_tile(snowflake_tile, fb_db_session, config):
    """
    Test generate_tiles method in TileSnowflake
    """
    snowflake_tile.generate_tiles(
        "ONLINE", "2022-06-05 23:33:00", "2022-06-05 23:58:00", credentials=config.credentials
    )

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = fb_db_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


def test_schedule_online_tile(snowflake_tile, fb_db_session, config):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    snowflake_tile.schedule_online_tiles(credentials=config.credentials)

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_ONLINE"

    result = fb_db_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3-59/5 * * * * UTC"
    assert result["state"].iloc[0] == "started"


def test_schedule_offline_tile(snowflake_tile, fb_db_session, config):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    snowflake_tile.schedule_offline_tiles(credentials=config.credentials)

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_OFFLINE"

    result = fb_db_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
    assert result["state"].iloc[0] == "started"


def test_insert_tile_registry(snowflake_tile, fb_db_session, config):
    """
    Test insert_tile_registry method in TileSnowflake
    """
    flag = snowflake_tile.insert_tile_registry(credentials=config.credentials)
    assert flag is True

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = fb_db_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert result["ENABLED"].iloc[0] == "Y"

    flag = snowflake_tile.insert_tile_registry(credentials=config.credentials)
    assert flag is False

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = fb_db_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert result["ENABLED"].iloc[0] == "Y"


def test_disable_tiles(snowflake_tile, fb_db_session, config):
    """
    Test disable_tiles method in TileSnowflake
    """
    flag = snowflake_tile.insert_tile_registry(credentials=config.credentials)
    assert flag is True

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = fb_db_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert result["ENABLED"].iloc[0] == "Y"

    # disable tile jobs
    snowflake_tile.disable_tiles(credentials=config.credentials)

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = fb_db_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert result["ENABLED"].iloc[0] == "N"
