"""
Test TileScheduler
"""
from datetime import datetime

import pytest

from featurebyte.tile.scheduler import TileScheduler


async def create_temp_file():
    pass


@pytest.mark.asyncio
async def test_tile_scheduler():
    """Test TileScheduler"""

    tile_scheduler = TileScheduler()

    job_name = "test_job_1"
    tile_scheduler.start_job_with_interval(
        job_id=job_name,
        interval_seconds=3,
        start_from=datetime.now(),
        func=create_temp_file,
    )

    assert job_name in tile_scheduler.get_jobs()

    tile_scheduler.stop_job(job_id=job_name)
    assert job_name not in tile_scheduler.get_jobs()
