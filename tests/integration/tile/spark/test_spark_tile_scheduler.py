"""
This module contains integration tests for TileManager scheduler
"""
import importlib
import json
from datetime import datetime

import pytest
from apscheduler.triggers.cron import CronTrigger

from featurebyte.common import date_util
from featurebyte.models.tile import TileType
from featurebyte.tile.celery.server import celery_instance
from featurebyte.tile.scheduler import TileSchedulerFactory


@pytest.fixture(name="tile_scheduler")
def mock_tile_scheduler_fixture():
    tile_scheduler = TileSchedulerFactory.get_instance()
    yield tile_scheduler
    celery_instance.shutdown()


@pytest.mark.parametrize("source_type", ["spark"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler(tile_spec, session, tile_manager, tile_scheduler):
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

    job_ids = tile_scheduler.get_jobs()
    assert job_id in job_ids

    job_details = tile_scheduler.get_job_details(job_id=job_id)

    # verifying scheduling trigger
    trigger = job_details.trigger
    assert isinstance(trigger, CronTrigger)
    scheduled = [int(str(f)) for f in trigger.fields if not f.is_default]
    expected = [f for f in next_job_time.timetuple()][:6]
    assert scheduled == expected

    # verifying the scheduling function
    module_path = job_details.func.__self__.args[0]
    class_name = job_details.func.__self__.args[1]
    instance_str = job_details.func.__self__.args[2]
    module = importlib.import_module(module_path)
    instance_class = getattr(module, class_name)
    instance_json = json.loads(instance_str)

    instance = instance_class(spark_session=session, **instance_json)
    await instance.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100
