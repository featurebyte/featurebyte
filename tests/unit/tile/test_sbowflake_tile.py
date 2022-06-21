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
    Pytest Fixture for SnowflakeConfig instance.
    """
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


def test_generate_tiles(snowflake_tile):
    """
    Test generate_tiles method in TileSnowflake
    """
    sql = snowflake_tile.generate_tiles("2022-06-20 15:00:00", "2022-06-21 15:00:00")
    expeted_sql = """
        call SP_TILE_GENERATE(
            'select c1 from dummy where tile_start_ts >= \\'2022-06-20 15:00:00\\' and tile_start_ts < \\'2022-06-21 15:00:00\\'',
            183, 3, 5, 'c1', 'tile_id1'
        )
    """
    assert "".join(sql.split()) == "".join(expeted_sql.split())
