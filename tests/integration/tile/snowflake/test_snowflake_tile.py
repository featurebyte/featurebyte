"""
This module contains integration tests for TileSnowflake
"""
from featurebyte.models.event_data import TileType


def test_generate_tile(snowflake_tile, snowflake_session):
    """
    Test generate_tiles method in TileSnowflake
    """
    snowflake_tile.generate_tiles(TileType.ONLINE, "2022-06-05 23:33:00", "2022-06-05 23:58:00")

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {snowflake_tile.tile_id}"
    result = snowflake_session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 5


def test_schedule_online_tile(snowflake_tile, snowflake_session):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    snowflake_tile.schedule_online_tiles()

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_ONLINE".upper()

    result = snowflake_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3-59/5 * * * * UTC"
    assert result["state"].iloc[0] == "started"


def test_schedule_offline_tile(snowflake_tile, snowflake_session):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    snowflake_tile.schedule_offline_tiles()

    task_name = f"SHELL_TASK_{snowflake_tile.tile_id}_OFFLINE".upper()

    result = snowflake_session.execute_query("SHOW TASKS")
    assert len(result) == 1
    assert result["name"].iloc[0] == task_name
    assert result["schedule"].iloc[0] == "USING CRON 3 0 * * * UTC"
    assert result["state"].iloc[0] == "started"


def test_insert_tile_registry(snowflake_tile, snowflake_session):
    """
    Test insert_tile_registry method in TileSnowflake
    """
    flag = snowflake_tile.insert_tile_registry()
    assert flag is True

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is True
    assert (
        result["TIME_MODULO_FREQUENCY_SECOND"].iloc[0]
        == snowflake_tile.time_modulo_frequency_seconds
    )

    flag = snowflake_tile.insert_tile_registry()
    assert flag is False

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is True
    assert (
        result["TIME_MODULO_FREQUENCY_SECOND"].iloc[0]
        == snowflake_tile.time_modulo_frequency_seconds
    )


def test_disable_tiles(snowflake_tile, snowflake_session):
    """
    Test disable_tiles method in TileSnowflake
    """
    flag = snowflake_tile.insert_tile_registry()
    assert flag is True

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is True
    assert (
        result["TIME_MODULO_FREQUENCY_SECOND"].iloc[0]
        == snowflake_tile.time_modulo_frequency_seconds
    )

    # disable tile jobs
    snowflake_tile.disable_tiles()

    sql = f"SELECT * FROM TILE_REGISTRY WHERE TILE_ID = '{snowflake_tile.tile_id}'"
    result = snowflake_session.execute_query(sql)
    assert len(result) == 1
    assert result["TILE_ID"].iloc[0] == snowflake_tile.tile_id
    assert bool(result["IS_ENABLED"].iloc[0]) is False
