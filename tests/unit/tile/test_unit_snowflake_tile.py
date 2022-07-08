"""
Unit test for snowflake tile
"""
import textwrap
from unittest import mock

import pytest

from featurebyte.models.tile import TileSpec, TileType


def test_construct_snowflaketile_time_modulo_error():
    """
    Test construct TileSpec validation error
    """
    with pytest.raises(ValueError) as excinfo:
        TileSpec(
            time_modulo_frequency_second=183,
            blind_spot_second=3,
            frequency_minute=3,
            tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
            column_names=["c1"],
            entity_column_names=["col1"],
        )
    assert "time_modulo_frequency_second must be less than 180" in str(excinfo.value)


def test_generate_tiles(mock_snowflake_tile, tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """
    sql = tile_manager.generate_tiles(
        mock_snowflake_tile,
        TileType.ONLINE,
        "2022-06-20 15:00:00",
        "2022-06-21 16:00:00",
        "2022-06-21 15:55:00",
    )
    expected_sql = textwrap.dedent(
        """
        call SP_TILE_GENERATE(
            'select c1 from dummy where tile_start_ts >= \\'2022-06-20 15:00:00\\' and tile_start_ts < \\'2022-06-21 16:00:00\\'',
            '__FB_TILE_START_DATE_COLUMN',
            183,
            3,
            5,
            'c1',
            'tile_id1',
            'ONLINE',
            '2022-06-21 15:55:00'
        )
        """
    ).strip()
    assert textwrap.dedent(sql).strip() == expected_sql


def test_schedule_online_tiles(mock_snowflake_tile, tile_manager):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    sql = tile_manager.schedule_online_tiles(mock_snowflake_tile)
    expected_sql = textwrap.dedent(
        """
        CREATE OR REPLACE TASK SHELL_TASK_tile_id1_ONLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3-59/5 * * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_tile_id1_ONLINE',
                'sf_warehouse',
                'tile_id1',
                183,
                3,
                5,
                1440,
                'select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE',
                '__FB_TILE_START_DATE_COLUMN',
                '__FB_START_DATE',
                '__FB_END_DATE',
                'c1',
                'ONLINE',
                10
            )
        """
    ).strip()
    assert textwrap.dedent(sql).strip() == expected_sql


def test_schedule_offline_tiles(mock_snowflake_tile, tile_manager):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    sql = tile_manager.schedule_offline_tiles(mock_snowflake_tile)
    expected_sql = textwrap.dedent(
        """
        CREATE OR REPLACE TASK SHELL_TASK_tile_id1_OFFLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3 0 * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_tile_id1_OFFLINE',
                'sf_warehouse',
                'tile_id1',
                183,
                3,
                5,
                1440,
                'select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE',
                '__FB_TILE_START_DATE_COLUMN',
                '__FB_START_DATE',
                '__FB_END_DATE',
                'c1',
                'OFFLINE',
                10
            )
        """
    ).strip()
    assert textwrap.dedent(sql).strip() == expected_sql


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
def test_insert_tile_registry(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    mock_execute_query.return_value = ["Element"]
    flag = tile_manager.insert_tile_registry(mock_snowflake_tile)
    assert flag is False

    mock_execute_query.return_value = []
    flag = tile_manager.insert_tile_registry(mock_snowflake_tile)
    assert flag is True
