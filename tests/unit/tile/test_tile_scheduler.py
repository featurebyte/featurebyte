"""
Test TileScheduler
"""
import asyncio
import os
from datetime import datetime

import pytest
import pytest_asyncio

from featurebyte.tile.scheduler import TileScheduler


@pytest_asyncio.fixture(name="tile_scheduler")
async def tile_scheduler_fixture():
    yield TileScheduler()

    job_file = "jobs.sqlite"
    if os.path.isfile(job_file):
        os.remove(job_file)


async def create_temp_file():
    tmp_file_name = "tmp_file"
    with open(tmp_file_name, "w"):
        pass


@pytest.mark.asyncio
async def test_tile_scheduler(tile_scheduler):
    """Test TileScheduler"""

    job_name = "test_job_1"
    tile_scheduler.start_job_with_interval(
        job_id=job_name,
        interval_seconds=3,
        start_from=datetime.now(),
        func=create_temp_file,
    )

    await asyncio.sleep(4)
    tmp_file_name = "tmp_file"
    assert os.path.exists(tmp_file_name)
    os.remove(tmp_file_name)

    assert job_name in tile_scheduler.get_jobs()

    tile_scheduler.stop_job(job_id=job_name)
    assert job_name not in tile_scheduler.get_jobs()
