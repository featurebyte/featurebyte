"""
Unit test for snowflake tile
"""
import pytest


@pytest.mark.parametrize(
    "val,lower,upper,error_msg",
    [
        (0, 1, -1, "0 must be an integer greater than or equal to 1"),
        (10, 0, 9, "10 must be an integer less than or equal to 9"),
    ],
)
def test_check_integer_range(mock_snowflake_tile, val, lower, upper, error_msg):
    """
    Test _check_integer_range method in TileBase
    """
    with pytest.raises(ValueError) as excinfo:
        mock_snowflake_tile.check_integer_range(val, lower, upper)

    assert str(excinfo.value) == error_msg


@pytest.mark.parametrize(
    "feature,time_modulo_frequency_seconds,blind_spot_seconds,frequency_minute,sql,col_names,tile_id,error_msg",
    [
        (" ", 183, 3, 5, "select dummy", "c1,c2", "tile_id1", "feature name cannot be empty"),
        ("f1", 183, 3, 5, " ", "c1,c2", "tile_id1", "tile_sql cannot be empty"),
        ("f1", 183, 3, 5, "select dummy", "", "tile_id1", "column_names cannot be empty"),
        ("f1", 183, 3, 5, "select dummy", "c1,c2", "", "tile_id cannot be empty"),
        (
            "f1",
            -183,
            3,
            3,
            "select dummy",
            "c1,c2",
            "tile_id1",
            "-183 must be an integer greater than or equal to 0",
        ),
        (
            "f1",
            183,
            3,
            3,
            "select dummy",
            "c1,c2",
            "tile_id1",
            "183 must be an integer less than or equal to 180",
        ),
    ],
)
def test_tile_validate(
    mock_snowflake_tile,
    feature,
    time_modulo_frequency_seconds,
    blind_spot_seconds,
    frequency_minute,
    sql,
    col_names,
    tile_id,
    error_msg,
):
    """
    Test validate method in TileBase
    """
    with pytest.raises(ValueError) as excinfo:
        mock_snowflake_tile.validate(
            feature,
            time_modulo_frequency_seconds,
            blind_spot_seconds,
            frequency_minute,
            sql,
            col_names,
            tile_id,
        )

    assert str(excinfo.value) == error_msg


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
