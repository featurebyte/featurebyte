"""
Unit test for snowflake tile
"""
from unittest import mock

import pytest

from featurebyte.tile.snowflake import TileSnowflake


@pytest.fixture
def mock_snowflake_session():
    """
    Pytest Fixture for Mock SnowflakeSession
    """
    with mock.patch("featurebyte.session.snowflake.SnowflakeSession") as m:
        yield m


@pytest.fixture
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def snowflake_tile(mock_execute_query, mock_snowflake_session):
    """
    Pytest Fixture for TileSnowflake instance
    """
    mock_snowflake_session.warehouse = "warehouse"
    mock_execute_query.size_effect = None

    tile_s = TileSnowflake(
        mock_snowflake_session,
        "featurename",
        183,
        3,
        5,
        "select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
        "c1",
        "tile_id1",
    )
    return tile_s


def test_check_integer_range(snowflake_tile):
    """
    Test _check_integer_range method in TileBase
    """
    # test validate non integer
    with pytest.raises(ValueError):
        snowflake_tile._check_integer_range(1.1, 1)

    # test value smaller than lower bound
    with pytest.raises(ValueError):
        snowflake_tile._check_integer_range(0, 1)

    # test integer greater than upper bound
    with pytest.raises(ValueError):
        snowflake_tile._check_integer_range(10, 0, 9)


def test_tile_validate(snowflake_tile):
    """
    Test validate method in TileBase
    """
    # test empty feature name
    with pytest.raises(ValueError):
        snowflake_tile.validate(" ", 183, 3, 5, "select dummy", "c1,c2", "tile_id1")

    # test empty tile sql
    with pytest.raises(ValueError):
        snowflake_tile.validate("f1", 183, 3, 5, " ", "c1,c2", "tile_id1")

    # test empty column names
    with pytest.raises(ValueError):
        snowflake_tile.validate("f1", 183, 3, 5, "select dummy", "", "tile_id1")

    # test empty tile id
    with pytest.raises(ValueError):
        snowflake_tile.validate("f1", 183, 3, 5, "select dummy", "c1,c2", "")

    # test empty negative time modulo frequency
    with pytest.raises(ValueError):
        snowflake_tile.validate("f1", -183, 3, 3, "select dummy", "c1,c2", "tile_id1")

    # test time modulo frequency greater than frequency
    with pytest.raises(ValueError):
        snowflake_tile.validate("f1", 183, 3, 3, "select dummy", "c1,c2", "tile_id1")


def test_generate_tiles(snowflake_tile):
    """
    Test generate_tiles method in TileSnowflake
    """
    sql = snowflake_tile.generate_tiles("2022-06-20 15:00:00", "2022-06-21 15:00:00")
    expected_sql = """
        call SP_TILE_GENERATE(
            'select c1 from dummy where tile_start_ts >= \\'2022-06-20 15:00:00\\' and tile_start_ts < \\'2022-06-21 15:00:00\\'',
            183, 3, 5, 'c1', 'tile_id1'
        )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_online_tiles(snowflake_tile):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    sql = snowflake_tile.schedule_online_tiles()
    expected_sql = """
        CREATE OR REPLACE TASK TASK_tile_id1_ONLINE
          WAREHOUSE = warehouse
          SCHEDULE = 'USING CRON */8 * * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'TASK_tile_id1_ONLINE', 'warehouse', 'tile_id1', 183, 3,
                5, 1440, 'select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS', 'c1', 'ONLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_offline_tiles(snowflake_tile):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    sql = snowflake_tile.schedule_offline_tiles()
    expected_sql = """
        CREATE OR REPLACE TASK TASK_tile_id1_OFFLINE
          WAREHOUSE = warehouse
          SCHEDULE = 'USING CRON 3 0 * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'TASK_tile_id1_OFFLINE', 'warehouse', 'tile_id1', 183, 3,
                5, 1440, 'select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS', 'c1', 'OFFLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())
