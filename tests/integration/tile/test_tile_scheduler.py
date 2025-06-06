"""
This module contains integration tests for TileManager scheduler
"""

# ruff:

from unittest import mock
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import pytest_asyncio

from featurebyte.models.periodic_task import Interval
from featurebyte.models.tile import TileType
from featurebyte.worker.task_executor import TaskExecutor


@pytest_asyncio.fixture(name="scheduler_fixture")
async def mock_scheduler_fixture(feature, tile_spec, tile_scheduler_service):
    """
    Fixture for TileScheduler information
    """
    tile_spec.tile_sql = "SELECT * FROM TEMP_TABLE"
    tile_spec.feature_store_id = feature.tabular_source.feature_store_id
    job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

    yield tile_scheduler_service, tile_spec, job_id

    await tile_scheduler_service.stop_job(job_id=job_id)


@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler__verify_scheduling_and_execution(
    feature_store, session, tile_manager_service, scheduler_fixture, app_container, persistent
):
    """
    Test generate_tiles with scheduler
    """
    _ = persistent

    tile_scheduler_service, tile_spec, job_id = scheduler_fixture

    await tile_manager_service.schedule_online_tiles(
        tile_spec=tile_spec, deployed_tile_table_id=None
    )

    job_details = await tile_scheduler_service.get_job_details(job_id=job_id)
    assert job_details is not None
    assert job_details.name == job_id
    assert job_details.time_modulo_frequency_second == tile_spec.time_modulo_frequency_second
    assert job_details.interval == Interval(every=tile_spec.frequency_minute * 60, period="seconds")

    task_id = uuid4()
    app_container.override_instances_for_test({
        "task_id": task_id,
        "progress": AsyncMock(),
    })
    task_executor = TaskExecutor(
        payload=job_details.kwargs, task_id=task_id, app_container=app_container
    )
    with mock.patch(
        "featurebyte.service.feature_store.FeatureStoreService.get_document"
    ) as mock_feature_store_service:
        with mock.patch("featurebyte.service.task_manager.TaskManager.get_task") as mock_get_task:
            mock_feature_store_service.return_value = feature_store
            mock_get_task.return_value = None
            await task_executor.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100


@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler__avoid_duplicate_tile(
    feature_store, session, tile_manager_service, scheduler_fixture
):
    """
    Test generate_tiles with scheduler - avoid duplicate tile job
    """
    tile_scheduler, tile_spec, job_id = scheduler_fixture
    sql = await tile_manager_service.schedule_online_tiles(
        tile_spec=tile_spec, deployed_tile_table_id=None
    )
    assert sql is not None

    sql = await tile_manager_service.schedule_online_tiles(
        tile_spec=tile_spec, deployed_tile_table_id=None
    )
    assert sql is None


@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler__tile_job_exists(
    feature_store, session, tile_manager_service, scheduler_fixture
):
    """
    Test generate_tiles with scheduler - test tile_job_exists
    """
    tile_scheduler, tile_spec, job_id = scheduler_fixture

    exists = await tile_manager_service.tile_job_exists(info=tile_spec)
    assert exists is False

    sql = await tile_manager_service.schedule_online_tiles(
        tile_spec=tile_spec, deployed_tile_table_id=None
    )
    assert sql is not None

    exists = await tile_manager_service.tile_job_exists(info=tile_spec)
    assert exists is True
