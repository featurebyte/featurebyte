"""
Test cases for FeatureStoreTableCleanupSchedulerService
"""

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID


@pytest.fixture
def catalog_id():
    """
    Fixture for catalog id
    """
    return ObjectId()


@pytest.fixture
def feature_store_id():
    """
    Fixture for feature store id
    """
    return ObjectId()


@pytest_asyncio.fixture
async def feature_store_table_cleanup_job(
    feature_store_table_cleanup_scheduler_service,
    feature_store_id,
):
    """
    Fixture for feature store table cleanup job
    """
    await feature_store_table_cleanup_scheduler_service.start_job_if_not_exist(feature_store_id)
    return await feature_store_table_cleanup_scheduler_service.get_periodic_task(feature_store_id)


@pytest.mark.asyncio
async def test_start_job__non_existing(
    feature_store_table_cleanup_scheduler_service,
    feature_store_id,
    user,
):
    """
    Test start_job_if_not_exist for non-existing job
    """
    task = await feature_store_table_cleanup_scheduler_service.get_periodic_task(feature_store_id)
    assert task is None

    await feature_store_table_cleanup_scheduler_service.start_job_if_not_exist(feature_store_id)
    task = await feature_store_table_cleanup_scheduler_service.get_periodic_task(feature_store_id)
    assert task is not None
    assert {
        "command": "FEATURE_STORE_CLEANUP",
        "user_id": str(user.id),
        "catalog_id": str(DEFAULT_CATALOG_ID),
        "feature_store_id": str(feature_store_id),
    }.items() <= task.kwargs.items()
    assert task.interval.every == 86400  # 24 hours in seconds
    assert task.interval.period == "seconds"
    assert task.time_modulo_frequency_second == 7200  # 2-hour offset


@pytest.mark.asyncio
async def test_start_job__existing(
    feature_store_table_cleanup_scheduler_service,
    feature_store_id,
    periodic_task_service,
):
    """
    Test starting a job when there is already a scheduled one
    """
    await feature_store_table_cleanup_scheduler_service.start_job_if_not_exist(feature_store_id)
    await feature_store_table_cleanup_scheduler_service.start_job_if_not_exist(feature_store_id)
    name = feature_store_table_cleanup_scheduler_service._get_job_id(feature_store_id)
    result = await periodic_task_service.list_documents_as_dict(
        page=1,
        page_size=0,
        query_filter={"name": name},
    )
    assert len(result["data"]) == 1


@pytest.mark.asyncio
async def test_stop_job(
    feature_store_table_cleanup_scheduler_service,
    feature_store_id,
    feature_store_table_cleanup_job,
):
    """
    Test stopping a job
    """
    _ = feature_store_table_cleanup_job
    await feature_store_table_cleanup_scheduler_service.stop_job(feature_store_id)
    task = await feature_store_table_cleanup_scheduler_service.get_periodic_task(feature_store_id)
    assert task is None


@pytest.mark.asyncio
async def test_get_job_id(feature_store_table_cleanup_scheduler_service, feature_store_id):
    """
    Test job ID generation
    """
    job_id = feature_store_table_cleanup_scheduler_service._get_job_id(feature_store_id)
    expected = f"feature_store_table_cleanup_{feature_store_id}"
    assert job_id == expected
