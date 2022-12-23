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
            value_column_names=["col2"],
            entity_column_names=["col1"],
        )
    assert "time_modulo_frequency_second must be less than 180" in str(excinfo.value)


def test_construct_snowflaketile_frequency_minute_error():
    """
    Test construct TileSpec validation error
    """
    with pytest.raises(ValueError) as excinfo:
        TileSpec(
            tile_id="tile_id_1",
            aggregation_id="agg_id1",
            time_modulo_frequency_second=150,
            blind_spot_second=3,
            frequency_minute=70,
            tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
            value_column_names=["col2"],
            entity_column_names=["col1"],
        )
    assert "frequency_minute should be a multiple of 60 if it is more than 60" in str(excinfo.value)


def test_construct_snowflaketile_zero_time_modulo_frequency():
    """
    Test construct TileSpec with time modulo frequency of 0
    """
    tile_spec = TileSpec(
        time_modulo_frequency_second=0,
        blind_spot_second=3,
        frequency_minute=3,
        tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
        value_column_names=["col2"],
        entity_column_names=["col1"],
        tile_id="some_tile_id",
        aggregation_id="some_agg_id",
    )
    assert tile_spec.time_modulo_frequency_second == 0
    assert tile_spec.blind_spot_second == 3
    assert tile_spec.frequency_minute == 3


@pytest.mark.asyncio
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def test_generate_tiles(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """
    _ = mock_execute_query
    sql = await tile_manager.generate_tiles(
        mock_snowflake_tile,
        TileType.ONLINE,
        "2022-06-20 15:00:00",
        "2022-06-21 16:00:00",
        "2022-06-21 15:55:00",
    )
    expected_sql = textwrap.dedent(
        """
        call SP_TILE_GENERATE(
            'select c1 from dummy where tile_start_ts >= ''2022-06-20 15:00:00'' and tile_start_ts < ''2022-06-21 16:00:00''',
            '__FB_TILE_START_DATE_COLUMN',
            'LAST_TILE_START_DATE',
            183,
            3,
            5,
            '"col1"',
            'col2',
            'TILE_ID1',
            'ONLINE',
            '2022-06-21 15:55:00'
        )
        """
    ).strip()
    assert textwrap.dedent(sql).strip() == expected_sql


@pytest.mark.asyncio
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def test_schedule_online_tiles(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    _ = mock_execute_query

    minute_offset = mock_snowflake_tile.time_modulo_frequency_second // 60

    sql = await tile_manager.schedule_online_tiles(mock_snowflake_tile)

    expected_sql = textwrap.dedent(
        f"""
        CREATE OR REPLACE TASK SHELL_TASK_TILE_ID1_ONLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON {minute_offset} * * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_TILE_ID1_ONLINE',
                'sf_warehouse',
                'TILE_ID1',
                183,
                3,
                5,
                1440,
                'select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE',
                '__FB_TILE_START_DATE_COLUMN',
                'LAST_TILE_START_DATE',
                '__FB_START_DATE',
                '__FB_END_DATE',
                '"col1"',
                'col2',
                'ONLINE',
                10
            )
        """
    ).strip()
    assert textwrap.dedent(sql).strip() == expected_sql


@pytest.mark.asyncio
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def test_schedule_offline_tiles(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    _ = mock_execute_query
    sql = await tile_manager.schedule_offline_tiles(mock_snowflake_tile)
    expected_sql = textwrap.dedent(
        """
        CREATE OR REPLACE TASK SHELL_TASK_TILE_ID1_OFFLINE
          WAREHOUSE = sf_warehouse
          SCHEDULE = 'USING CRON 3 0 * * * UTC'
        AS
            call SP_TILE_TRIGGER_GENERATE_SCHEDULE(
                'SHELL_TASK_TILE_ID1_OFFLINE',
                'sf_warehouse',
                'TILE_ID1',
                183,
                3,
                5,
                1440,
                'select c1 from dummy where tile_start_ts >= __FB_START_DATE and tile_start_ts < __FB_END_DATE',
                '__FB_TILE_START_DATE_COLUMN',
                'LAST_TILE_START_DATE',
                '__FB_START_DATE',
                '__FB_END_DATE',
                '"col1"',
                'col2',
                'OFFLINE',
                10
            )
        """
    ).strip()
    assert textwrap.dedent(sql).strip() == expected_sql


@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
@pytest.mark.asyncio
async def test_insert_tile_registry(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    mock_execute_query.return_value = ["Element"]
    flag = await tile_manager.insert_tile_registry(mock_snowflake_tile)
    assert flag is False

    mock_execute_query.return_value = []
    flag = await tile_manager.insert_tile_registry(mock_snowflake_tile)
    assert flag is True


@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.generate_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.update_tile_entity_tracker")
@pytest.mark.asyncio
async def test_generate_tiles_on_demand(
    mock_generate_tiles,
    mock_update_tile_entity_tracker,
    mock_snowflake_tile,
    tile_manager,
):
    """
    Test generate_tiles_on_demand
    """
    mock_generate_tiles.size_effect = None
    mock_update_tile_entity_tracker.size_effect = None

    await tile_manager.generate_tiles_on_demand([(mock_snowflake_tile, "temp_entity_table")])

    mock_generate_tiles.assert_called_once()
    mock_update_tile_entity_tracker.assert_called_once()
