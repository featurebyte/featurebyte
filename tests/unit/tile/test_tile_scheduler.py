"""
Test TileScheduler
"""
from datetime import datetime

import pytest
from pydantic import BaseModel

from featurebyte.tile.celery.server import celery_instance
from featurebyte.tile.scheduler import TileSchedulerFactory


class MockTestClass(BaseModel):
    """
    Simulated class for Tile Operation Classes
    """

    async def execute(self) -> None:
        """
        Execute tile generate operation
        """
        pass


@pytest.fixture(name="mock_tile_scheduler")
def mock_tile_scheduler_fixture():
    tile_scheduler = TileSchedulerFactory.get_instance()
    yield tile_scheduler
    celery_instance.shutdown()


async def create_temp_file():
    pass


@pytest.mark.asyncio
async def test_tile_scheduler(mock_tile_scheduler):
    """Test TileScheduler"""

    instance = MockTestClass()
    job_name = "test_job_1"
    mock_tile_scheduler.start_job_with_interval(
        job_id=job_name,
        interval_seconds=3,
        start_from=datetime.now(),
        instance=instance,
    )

    assert job_name in mock_tile_scheduler.get_jobs()

    mock_tile_scheduler.stop_job(job_id=job_name)
    assert job_name not in mock_tile_scheduler.get_jobs()
