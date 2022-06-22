"""
Unit test for snowflake tile
"""
import pytest


def test_check_integer_range(mock_snowflake_tile):
    """
    Test _check_integer_range method in TileBase
    """
    # test value smaller than lower bound
    with pytest.raises(ValueError):
        mock_snowflake_tile.check_integer_range(0, 1)

    # test integer greater than upper bound
    with pytest.raises(ValueError):
        mock_snowflake_tile.check_integer_range(10, 0, 9)


def test_tile_validate(mock_snowflake_tile):
    """
    Test validate method in TileBase
    """
    # test empty feature name
    with pytest.raises(ValueError):
        mock_snowflake_tile.validate(" ", 183, 3, 5, "select dummy", "c1,c2", "tile_id1")

    # test empty tile sql
    with pytest.raises(ValueError):
        mock_snowflake_tile.validate("f1", 183, 3, 5, " ", "c1,c2", "tile_id1")

    # test empty column names
    with pytest.raises(ValueError):
        mock_snowflake_tile.validate("f1", 183, 3, 5, "select dummy", "", "tile_id1")

    # test empty tile id
    with pytest.raises(ValueError):
        mock_snowflake_tile.validate("f1", 183, 3, 5, "select dummy", "c1,c2", "")

    # test empty negative time modulo frequency
    with pytest.raises(ValueError):
        mock_snowflake_tile.validate("f1", -183, 3, 3, "select dummy", "c1,c2", "tile_id1")

    # test time modulo frequency greater than frequency
    with pytest.raises(ValueError):
        mock_snowflake_tile.validate("f1", 183, 3, 3, "select dummy", "c1,c2", "tile_id1")


def test_generate_tiles(mock_snowflake_tile):
    """
    Test generate_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.generate_tiles("2022-06-20 15:00:00", "2022-06-21 15:00:00")
    expected_sql = """
        call SP_TILE_GENERATE(
            'select c1 from dummy
            where tile_start_ts >= \\'2022-06-20 15:00:00\\'
            and tile_start_ts < \\'2022-06-21 15:00:00\\'',
            183, 3, 5, 'C1', 'TILE_ID1'
        )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_online_tiles(mock_snowflake_tile):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.schedule_online_tiles()
    expected_sql = """
        CREATE OR REPLACE TASK SHELL_TASK_TILE_ID1_ONLINE
          WAREHOUSE = warehouse
          SCHEDULE = 'USING CRON 3-59/5 * * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_TILE_ID1_ONLINE', 'warehouse', 'TILE_ID1', 183,
                3, 5, 1440, 'select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS',
                'C1',
                'ONLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_offline_tiles(mock_snowflake_tile):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.schedule_offline_tiles()
    expected_sql = """
        CREATE OR REPLACE TASK SHELL_TASK_TILE_ID1_OFFLINE
          WAREHOUSE = warehouse
          SCHEDULE = 'USING CRON 3 0 * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_TILE_ID1_OFFLINE', 'warehouse', 'TILE_ID1', 183,
                3, 5, 1440, 'select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS',
                'C1',
                'OFFLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())
