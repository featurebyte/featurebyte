"""
This module contains unit tests for FeatureListManagerSnowflake
"""
from unittest import mock

import pytest


@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.generate_tiles")
@mock.patch("featurebyte.tile.snowflake_tile.TileManagerSnowflake.update_tile_entity_tracker")
@pytest.mark.asyncio
async def test_generate_tiles_on_demand(
    mock_generate_tiles,
    mock_update_tile_entity_tracker,
    mock_snowflake_tile,
    feature_list_manager,
):
    """
    Test generate_tiles_on_demand
    """
    mock_generate_tiles.size_effect = None
    mock_update_tile_entity_tracker.size_effect = None

    await feature_list_manager.generate_tiles_on_demand(
        [(mock_snowflake_tile, "temp_entity_table")]
    )

    mock_generate_tiles.assert_called_once()
    mock_update_tile_entity_tracker.assert_called_once()
