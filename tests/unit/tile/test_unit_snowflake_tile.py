"""
Unit test for snowflake tile
"""
from unittest import mock
from unittest.mock import Mock

import pytest
from bson import ObjectId

from featurebyte import SourceType
from featurebyte.models.tile import TileSpec, TileType
from featurebyte.session.snowflake import SnowflakeSession


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
            feature_store_id=ObjectId(),
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
            feature_store_id=ObjectId(),
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
        feature_store_id=ObjectId(),
    )
    assert tile_spec.time_modulo_frequency_second == 0
    assert tile_spec.blind_spot_second == 3
    assert tile_spec.frequency_minute == 3


@pytest.mark.asyncio
async def test_schedule_online_tiles(
    mock_snowflake_tile, tile_manager_service, mock_snowflake_session
):
    """
    Test schedule_online_tiles method in TileSnowflake
    """
    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService._schedule_tiles_custom"
    ) as mock_schedule_tiles_custom:
        await tile_manager_service.schedule_online_tiles(
            mock_snowflake_session, mock_snowflake_tile
        )
        kwargs = mock_schedule_tiles_custom.call_args.kwargs
        assert kwargs["tile_spec"] == mock_snowflake_tile
        assert kwargs["tile_type"] == TileType.ONLINE
        assert kwargs["monitor_periods"] == 10


@pytest.fixture(name="tile_manager_service")
def tile_manager_service_fixture(app_container):
    """
    TileManagerService object fixture
    """
    return app_container.tile_manager_service


@pytest.fixture(name="mock_snowflake_session")
def mock_snowflake_session_fixture():
    """
    SnowflakeSession object fixture
    """
    return Mock(
        name="mock_snowflake_session",
        spec=SnowflakeSession,
        source_type=SourceType.SNOWFLAKE,
    )


@pytest.mark.asyncio
async def test_schedule_offline_tiles(
    mock_snowflake_session, mock_snowflake_tile, tile_manager_service
):
    """
    Test schedule_offline_tiles method in TileSnowflake
    """
    with mock.patch(
        "featurebyte.service.tile_manager.TileManagerService._schedule_tiles_custom"
    ) as mock_schedule_tiles_custom:
        await tile_manager_service.schedule_offline_tiles(
            mock_snowflake_session, tile_spec=mock_snowflake_tile
        )
        kwargs = mock_schedule_tiles_custom.call_args.kwargs
        assert kwargs["tile_spec"] == mock_snowflake_tile
        assert kwargs["tile_type"] == TileType.OFFLINE
        assert kwargs["offline_minutes"] == 1440


@mock.patch("featurebyte.service.tile_manager.TileManagerService.generate_tiles")
@mock.patch("featurebyte.service.tile_manager.TileManagerService.update_tile_entity_tracker")
@pytest.mark.asyncio
async def test_generate_tiles_on_demand(
    mock_generate_tiles,
    mock_update_tile_entity_tracker,
    mock_snowflake_tile,
    tile_manager_service,
    mock_snowflake_session,
):
    """
    Test generate_tiles_on_demand
    """
    mock_generate_tiles.size_effect = None
    mock_update_tile_entity_tracker.size_effect = None

    await tile_manager_service.generate_tiles_on_demand(
        mock_snowflake_session, [(mock_snowflake_tile, "temp_entity_table")]
    )

    mock_generate_tiles.assert_called_once()
    mock_update_tile_entity_tracker.assert_called_once()
