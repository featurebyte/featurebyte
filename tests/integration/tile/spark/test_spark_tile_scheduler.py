"""
This module contains integration tests for TileManager scheduler
"""
from datetime import datetime, timedelta

import pytest
from apscheduler.triggers.interval import IntervalTrigger

from featurebyte.common import date_util
from featurebyte.models.tile import TileType
from featurebyte.tile.scheduler import TileSchedulerFactory


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler(tile_spec, session, tile_manager):
    """
    Test generate_tiles method in TileSnowflake
    """
    schedule_time = datetime.utcnow()
    next_job_time = date_util.get_next_job_datetime(
        input_dt=schedule_time,
        frequency_minutes=tile_spec.frequency_minute,
        time_modulo_frequency_seconds=tile_spec.time_modulo_frequency_second,
    )
    tile_spec.tile_sql = "SELECT * FROM TEMP_TABLE"

    await tile_manager.schedule_online_tiles(tile_spec=tile_spec, schedule_time=schedule_time)
    job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

    tile_scheduler = TileSchedulerFactory.get_instance()
    job_ids = tile_scheduler.get_jobs()
    assert job_id in job_ids

    job_details = tile_scheduler.get_job_details(job_id=job_id)

    # verifying scheduling trigger
    interval_trigger = job_details.trigger
    assert isinstance(interval_trigger, IntervalTrigger)
    assert interval_trigger.interval == timedelta(minutes=tile_spec.frequency_minute)
    assert interval_trigger.start_date.strftime("%Y-%m-%d %H:%M:%S") == next_job_time.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # verifying scheduling function
    await job_details.func()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100
