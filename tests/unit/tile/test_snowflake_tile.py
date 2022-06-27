"""
Unit test for snowflake tile
"""
from unittest import mock

import pytest

from featurebyte.tile.snowflake_tile import TileSnowflake


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_construct_snowflaketile_time_modulo_error(mock_execute_query, snowflake_database_source):
    """
    Pytest Fixture for TileSnowflake instance
    """
    mock_execute_query.size_effect = None

    with pytest.raises(ValueError) as excinfo:
        TileSnowflake(
            feature_name="featurename",
            time_modulo_frequency_seconds=183,
            blind_spot_seconds=3,
            frequency_minute=3,
            tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
            column_names="c1",
            tile_id="tile_id1",
            tabular_source=snowflake_database_source,
        )
    assert "time_modulo_frequency_seconds must be less than 180" in str(excinfo.value)


def test_generate_tiles(mock_snowflake_tile, config):
    """
    Test generate_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.generate_tiles(
        "ONLINE", "2022-06-20 15:00:00", "2022-06-21 15:00:00", credentials=config.credentials
    )
    expected_sql = """
        call SP_TILE_GENERATE(
            'select c1 from dummy
            where tile_start_ts >= \\'2022-06-20 15:00:00\\'
            and tile_start_ts < \\'2022-06-21 15:00:00\\'',
            183, 3, 5, 'C1', 'TILE_ID1', 'ONLINE'
        )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_online_tiles(mock_snowflake_tile, config):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.schedule_online_tiles(credentials=config.credentials)
    expected_sql = """
        CREATE OR REPLACE TASK SHELL_TASK_TILE_ID1_ONLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3-59/5 * * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_TILE_ID1_ONLINE', 'sf_warehouse', 'TILE_ID1', 183, 3,
                5, 1440, 'select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS', 'C1', 'ONLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_offline_tiles(mock_snowflake_tile, config):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.schedule_offline_tiles(credentials=config.credentials)
    expected_sql = """
        CREATE OR REPLACE TASK SHELL_TASK_TILE_ID1_OFFLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3 0 * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_TILE_ID1_OFFLINE', 'sf_warehouse', 'TILE_ID1', 183, 3,
                5, 1440, 'select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS', 'C1', 'OFFLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())
