"""
Test cases for OnlineStoreCleanupSchedulerService
"""
import pytest
import pytest_asyncio
from bson import ObjectId


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


@pytest.fixture
def online_store_table_name():
    """
    Fixture for online store table name
    """
    return "online_store_1"


@pytest_asyncio.fixture
async def online_store_cleanup_job(
    online_store_cleanup_scheduler_service,
    catalog_id,
    feature_store_id,
    online_store_table_name,
):
    """
    Fixture for online store cleanup job
    """
    await online_store_cleanup_scheduler_service.start_job_if_not_exist(
        catalog_id, feature_store_id, online_store_table_name
    )
    return await online_store_cleanup_scheduler_service.get_periodic_task(online_store_table_name)


@pytest.mark.asyncio
async def test_start_job__non_existing(
    online_store_cleanup_scheduler_service,
    catalog_id,
    feature_store_id,
    online_store_table_name,
):
    """
    Test start_job_if_not_exist for non-existing job
    """
    task = await online_store_cleanup_scheduler_service.get_periodic_task(online_store_table_name)
    assert task is None

    await online_store_cleanup_scheduler_service.start_job_if_not_exist(
        catalog_id, feature_store_id, online_store_table_name
    )
    task = await online_store_cleanup_scheduler_service.get_periodic_task(online_store_table_name)
    assert task is not None
    assert {
        "command": "ONLINE_STORE_TABLE_CLEANUP",
        "catalog_id": str(catalog_id),
        "feature_store_id": str(feature_store_id),
        "online_store_table_name": "online_store_1",
    }.items() <= task.kwargs.items()
    assert task.interval.every == 86400
    assert task.interval.period == "seconds"


@pytest.mark.asyncio
async def test_start_job__existing(
    online_store_cleanup_scheduler_service,
    catalog_id,
    feature_store_id,
    online_store_table_name,
    periodic_task_service,
):
    """
    Test starting a job when there is already a scheduled one
    """
    await online_store_cleanup_scheduler_service.start_job_if_not_exist(
        catalog_id, feature_store_id, online_store_table_name
    )
    await online_store_cleanup_scheduler_service.start_job_if_not_exist(
        catalog_id, feature_store_id, online_store_table_name
    )
    name = online_store_cleanup_scheduler_service._get_job_id(online_store_table_name)
    result = await periodic_task_service.list_documents_as_dict(
        page=1,
        page_size=0,
        query_filter={"name": name},
    )
    assert len(result["data"]) == 1


@pytest.mark.asyncio
async def test_stop_job(
    online_store_cleanup_scheduler_service,
    online_store_table_name,
    online_store_cleanup_job,
):
    """
    Test stopping a job
    """
    _ = online_store_cleanup_job
    await online_store_cleanup_scheduler_service.stop_job(online_store_table_name)
    task = await online_store_cleanup_scheduler_service.get_periodic_task(online_store_table_name)
    assert task is None
