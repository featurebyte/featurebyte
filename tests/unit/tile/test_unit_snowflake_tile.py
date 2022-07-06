"""
Unit test for snowflake tile
"""
from unittest import mock

import pytest

from featurebyte.enum import SpecialColumnName
from featurebyte.models.feature import TileType
from featurebyte.tile.snowflake_tile import TileSnowflake


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_construct_snowflaketile_time_modulo_error(mock_execute_query, snowflake_feature_store):
    """
    Pytest Fixture for TileSnowflake instance
    """
    mock_execute_query.size_effect = None

    with pytest.raises(ValueError) as excinfo:
        TileSnowflake(
            time_modulo_frequency_seconds=183,
            blind_spot_seconds=3,
            frequency_minute=3,
            tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
            column_names="c1",
            tile_id="tile_id1",
            tabular_source=snowflake_feature_store,
        )
    assert "time_modulo_frequency_seconds must be less than 180" in str(excinfo.value)


def test_generate_tiles(mock_snowflake_tile):
    """
    Test generate_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.generate_tiles(
        TileType.ONLINE, "2022-06-20 15:00:00", "2022-06-21 16:00:00", "2022-06-21 15:55:00"
    )
    expected_sql = """
        call SP_TILE_GENERATE(
            'select c1 from dummy
            where tile_start_ts >= \\'2022-06-20 15:00:00\\'
            and tile_start_ts < \\'2022-06-21 16:00:00\\'',
            183, 3, 5, 'c1', 'tile_id1', 'ONLINE', '2022-06-21 15:55:00'
        )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_online_tiles(mock_snowflake_tile):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.schedule_online_tiles()
    start_placeholder = SpecialColumnName.TILE_START_DATE_SQL_PLACEHOLDER
    end_placeholder = SpecialColumnName.TILE_END_DATE_SQL_PLACEHOLDER
    expected_sql = f"""
        CREATE OR REPLACE TASK SHELL_TASK_tile_id1_ONLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3-59/5 * * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_tile_id1_ONLINE', 'sf_warehouse', 'tile_id1', 183, 3,
                5, 1440, 'select c1 from dummy where tile_start_ts >= {start_placeholder} and tile_start_ts < {end_placeholder}', 'c1', 'ONLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


def test_schedule_offline_tiles(mock_snowflake_tile):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    sql = mock_snowflake_tile.schedule_offline_tiles()
    start_placeholder = SpecialColumnName.TILE_START_DATE_SQL_PLACEHOLDER
    end_placeholder = SpecialColumnName.TILE_END_DATE_SQL_PLACEHOLDER
    expected_sql = f"""
        CREATE OR REPLACE TASK SHELL_TASK_tile_id1_OFFLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3 0 * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_tile_id1_OFFLINE', 'sf_warehouse', 'tile_id1', 183, 3,
                5, 1440, 'select c1 from dummy where tile_start_ts >= {start_placeholder} and tile_start_ts < {end_placeholder}', 'c1', 'OFFLINE', 10
            )
    """
    assert "".join(sql.split()) == "".join(expected_sql.split())


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_insert_tile_registry(mock_execute_query, mock_snowflake_tile):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    mock_execute_query.return_value = ["Element"]
    flag = mock_snowflake_tile.insert_tile_registry()
    assert flag is False

    mock_execute_query.return_value = []
    flag = mock_snowflake_tile.insert_tile_registry()
    assert flag is True
