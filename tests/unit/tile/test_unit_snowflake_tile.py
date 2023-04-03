"""
Unit test for snowflake tile
"""
import textwrap
from datetime import datetime
from unittest import mock

import pytest

from featurebyte.common import date_util
from featurebyte.feature_manager.sql_template import tm_call_schedule_online_store
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
            value_column_types=["FLOAT"],
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
            value_column_types=["FLOAT"],
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
        value_column_types=["FLOAT"],
        entity_column_names=["col1"],
        tile_id="some_tile_id",
        aggregation_id="some_agg_id",
    )
    assert tile_spec.time_modulo_frequency_second == 0
    assert tile_spec.blind_spot_second == 3
    assert tile_spec.frequency_minute == 3


@pytest.mark.asyncio
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def test_populate_feature_store(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test populate_feature_store method in TileSnowflake
    """
    job_schedule_ts_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    await tile_manager.populate_feature_store(mock_snowflake_tile, job_schedule_ts_str)
    expected_sql = textwrap.dedent(
        tm_call_schedule_online_store.render(
            aggregation_id=mock_snowflake_tile.aggregation_id,
            job_schedule_ts_str=job_schedule_ts_str,
        )
    ).strip()
    assert mock_execute_query.call_count == 1
    args, _ = mock_execute_query.call_args
    assert args[0].strip() == expected_sql


@pytest.mark.asyncio
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def test_schedule_online_tiles(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    _ = mock_execute_query

    schedule_time = datetime.utcnow()
    next_job_time = date_util.get_next_job_datetime(
        input_dt=schedule_time,
        frequency_minutes=mock_snowflake_tile.frequency_minute,
        time_modulo_frequency_seconds=mock_snowflake_tile.time_modulo_frequency_second,
    )

    with mock.patch(
        "featurebyte.tile.base.BaseTileManager._schedule_tiles_custom"
    ) as mock_schedule_tiles_custom:
        await tile_manager.schedule_online_tiles(mock_snowflake_tile, schedule_time=schedule_time)
        kwargs = mock_schedule_tiles_custom.call_args.kwargs
        assert kwargs["next_job_time"] == next_job_time


@pytest.mark.asyncio
@mock.patch("featurebyte.session.snowflake.SnowflakeSession.execute_query")
async def test_schedule_offline_tiles(mock_execute_query, mock_snowflake_tile, tile_manager):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    _ = mock_execute_query

    schedule_time = datetime.utcnow()
    next_job_time = date_util.get_next_job_datetime(
        input_dt=schedule_time,
        frequency_minutes=1440,
        time_modulo_frequency_seconds=mock_snowflake_tile.time_modulo_frequency_second,
    )

    with mock.patch(
        "featurebyte.tile.base.BaseTileManager._schedule_tiles_custom"
    ) as mock_schedule_tiles_custom:
        await tile_manager.schedule_offline_tiles(
            tile_spec=mock_snowflake_tile, schedule_time=schedule_time
        )
        kwargs = mock_schedule_tiles_custom.call_args.kwargs
        assert kwargs["next_job_time"] == next_job_time


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
