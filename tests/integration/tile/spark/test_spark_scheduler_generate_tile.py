"""
This module contains integration tests for TileManager scheduler
"""
import pytest

from featurebyte.enum import InternalName
from featurebyte.models.tile import TileType
from featurebyte.tile.scheduler import TileSchedulerFactory


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_online_tiles(tile_spec, session, tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """

    await tile_manager.schedule_online_tiles(tile_spec=tile_spec)
    job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

    tile_scheduler = TileSchedulerFactory.get_instance()
    job_idss = tile_scheduler.get_jobs()
    assert job_id in job_idss
