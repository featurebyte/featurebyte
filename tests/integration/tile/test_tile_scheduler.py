"""
This module contains integration tests for TileManager scheduler
"""
from datetime import datetime
from unittest import mock

import pytest
import pytest_asyncio

from featurebyte.common import date_util
from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.periodic_task import Interval
from featurebyte.models.tile import TileType
from featurebyte.service.task_manager import TaskManager
from featurebyte.tile.scheduler import TileScheduler
from featurebyte.worker.task_executor import TaskExecutor


@pytest_asyncio.fixture(name="scheduler_fixture")
async def mock_scheduler_fixture(feature, tile_spec, persistent):
    """
    Fixture for TileScheduler information
    """
    tile_spec.tile_sql = "SELECT * FROM TEMP_TABLE"
    tile_spec.user_id = feature.user_id
    tile_spec.catalog_id = feature.catalog_id
    tile_spec.feature_store_id = feature.tabular_source.feature_store_id
    job_id = f"{TileType.ONLINE}_{tile_spec.aggregation_id}"

    task_manager = TaskManager(
        user=User(id=feature.user_id), persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
    )
    tile_scheduler = TileScheduler(task_manager=task_manager)

    yield tile_scheduler, tile_spec, job_id

    await tile_scheduler.stop_job(job_id=job_id)


@pytest.mark.parametrize("source_type", ["spark", "snowflake", "databricks"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler__verify_scheduling_and_execution(
    feature_store, session, tile_manager, scheduler_fixture
):
    """
    Test generate_tiles with scheduler
    """
    tile_scheduler, tile_spec, job_id = scheduler_fixture

    await tile_manager.schedule_online_tiles(tile_spec=tile_spec)

    job_details = await tile_scheduler.get_job_details(job_id=job_id)
    assert job_details is not None
    assert job_details.name == job_id
    assert job_details.time_modulo_frequency_second == tile_spec.time_modulo_frequency_second
    assert job_details.interval == Interval(every=tile_spec.frequency_minute * 60, period="seconds")

    task_executor = TaskExecutor(payload=job_details.kwargs)
    with mock.patch(
        "featurebyte.service.feature_store.FeatureStoreService.get_document"
    ) as mock_feature_store_service:
        mock_feature_store_service.return_value = feature_store
        await task_executor.execute()

    sql = f"SELECT COUNT(*) as TILE_COUNT FROM {tile_spec.tile_id}"
    result = await session.execute_query(sql)
    assert result["TILE_COUNT"].iloc[0] == 100


@pytest.mark.parametrize("source_type", ["spark", "snowflake", "databricks"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler__avoid_duplicate_tile(
    feature_store, tile_manager, scheduler_fixture
):
    """
    Test generate_tiles with scheduler - avoid duplicate tile job
    """
    tile_scheduler, tile_spec, job_id = scheduler_fixture
    sql = await tile_manager.schedule_online_tiles(tile_spec=tile_spec)
    assert sql is not None

    sql = await tile_manager.schedule_online_tiles(tile_spec=tile_spec)
    assert sql is None


@pytest.mark.parametrize("source_type", ["spark", "snowflake", "databricks"], indirect=True)
@pytest.mark.asyncio
async def test_generate_tiles_with_scheduler__tile_job_exists(
    feature_store, tile_manager, scheduler_fixture
):
    """
    Test generate_tiles with scheduler - test tile_job_exists
    """
    tile_scheduler, tile_spec, job_id = scheduler_fixture

    exists = await tile_manager.tile_job_exists(tile_spec=tile_spec)
    assert exists is False

    sql = await tile_manager.schedule_online_tiles(tile_spec=tile_spec)
    assert sql is not None

    exists = await tile_manager.tile_job_exists(tile_spec=tile_spec)
    assert exists is True
