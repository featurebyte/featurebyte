"""
Unit test for snowflake tile
"""

from unittest import mock
from unittest.mock import AsyncMock, Mock, call

import pytest
from bson import ObjectId

from featurebyte.models.tile import OnDemandTileSpec, TileSpec, TileType
from featurebyte.query_graph.sql.tile_compute_combine import TileTableGrouping
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


def test_construct_snowflaketile_time_modulo_error():
    """
    Test construct TileSpec validation error
    """
    with pytest.raises(ValueError) as excinfo:
        TileSpec(
            tile_id="tile_id_1",
            aggregation_id="agg_id1",
            time_modulo_frequency_second=183,
            blind_spot_second=3,
            frequency_minute=3,
            tile_sql="select c1 from dummy where tile_start_ts >= FB_START_TS and tile_start_ts < FB_END_TS",
            value_column_names=["col2"],
            value_column_types=["FLOAT"],
            entity_column_names=["col1"],
            feature_store_id=ObjectId(),
            windows=["1d"],
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
            windows=["1d"],
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
        windows=["1d"],
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
            tile_spec=mock_snowflake_tile, deployed_tile_table_id=None
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
            tile_spec=mock_snowflake_tile, deployed_tile_table_id=None
        )
        kwargs = mock_schedule_tiles_custom.call_args.kwargs
        assert kwargs["tile_spec"] == mock_snowflake_tile
        assert kwargs["tile_type"] == TileType.OFFLINE
        assert kwargs["offline_minutes"] == 1440


@mock.patch("featurebyte.service.feature_store.FeatureStoreService.get_document")
@pytest.mark.asyncio
async def test_generate_tiles_on_demand__observation_table_id(
    mock_feature_store_service_get_document,
    mock_snowflake_tile,
    tile_manager_service,
    mock_snowflake_session,
    update_fixtures,
):
    """
    Test generate_tiles_on_demand for observation table
    """
    mock_feature_store_service_get_document.return_value = Mock(
        id=mock_snowflake_tile.feature_store_id, max_query_concurrency=None
    )

    observation_table_id = ObjectId()
    await tile_manager_service.generate_tiles_on_demand(
        mock_snowflake_session,
        [
            OnDemandTileSpec(
                tile_spec=mock_snowflake_tile,
                observation_table_id=observation_table_id,
                tile_table_groupings=[
                    TileTableGrouping(
                        aggregation_id="agg_id_1",
                        tile_id="tile_id_1",
                        value_column_names=["col1"],
                        value_column_types=["FLOAT"],
                    ),
                    TileTableGrouping(
                        aggregation_id="agg_id_2",
                        tile_id="tile_id_2",
                        value_column_names=["col2"],
                        value_column_types=["FLOAT"],
                    ),
                ],
            )
        ],
        "some_tag",
    )

    queries = extract_session_executed_queries(mock_snowflake_session)
    assert_equal_with_expected_fixture(
        queries,
        "tests/fixtures/generate_tiles_on_demand_for_obs_table.sql",
        update_fixtures,
    )


@mock.patch("featurebyte.service.tile_manager.TileManagerService.generate_tiles")
@mock.patch("featurebyte.service.feature_store.FeatureStoreService.get_document")
@pytest.mark.asyncio
async def test_generate_tiles_on_demand__progress_update(
    mock_feature_store_service_get_document,
    mock_generate_tiles,
    mock_snowflake_tile,
    tile_manager_service,
    mock_snowflake_session,
):
    """
    Test generate_tiles_on_demand
    """
    mock_generate_tiles.side_effect = None
    mock_feature_store_service_get_document.return_value = Mock(
        id=mock_snowflake_tile.feature_store_id, max_query_concurrency=None
    )
    mock_progress_callback = AsyncMock()

    on_demand_tile_spec = OnDemandTileSpec(
        tile_spec=mock_snowflake_tile,
        tile_table_groupings=[
            TileTableGrouping(
                aggregation_id="agg_id_1",
                tile_id="tile_id_1",
                value_column_names=["col1"],
                value_column_types=["FLOAT"],
            ),
        ],
    )
    await tile_manager_service.generate_tiles_on_demand(
        mock_snowflake_session,
        [
            on_demand_tile_spec,
            on_demand_tile_spec,
            on_demand_tile_spec,
            on_demand_tile_spec,
        ],
        "some_tag",
        progress_callback=mock_progress_callback,
    )

    assert mock_progress_callback.call_args_list == [
        call(0, "Computed 0 out of 4 tile tables"),
        call(25, "Computed 1 out of 4 tile tables"),
        call(50, "Computed 2 out of 4 tile tables"),
        call(75, "Computed 3 out of 4 tile tables"),
        call(100, "Computed 4 out of 4 tile tables"),
    ]
