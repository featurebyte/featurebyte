"""
Tests for process store
"""

import asyncio
import datetime
import time

import pytest
import pytz

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.periodic_task import Crontab, Interval
from featurebyte.models.task import LogMessage, ProgressHistory, Task
from featurebyte.schema.task import TaskId, TaskStatus
from featurebyte.schema.worker.task.test import TestIOTaskPayload, TestTaskPayload
from featurebyte.service.task_manager import TaskManager


async def wait_for_async_task(
    task_manager: TaskManager, task_id: TaskId, timeout_seconds=30
) -> None:
    """
    Wait for async task to finish
    """
    start_time = time.time()
    while (time.time() - start_time) < timeout_seconds:
        task = await task_manager.get_task(task_id=task_id)
        if task.status in TaskStatus.terminal():
            return task
        await asyncio.sleep(1)
    raise TimeoutError("Timeout waiting for task to finish")


@pytest.fixture(name="task_manager")
def task_manager_fixture(celery_service, app_container_no_catalog):
    """Task manager fixture"""
    persistent, celery = celery_service
    app_container_no_catalog.override_instance_for_test("persistent", persistent)
    app_container_no_catalog.override_instance_for_test("celery", celery)
    return app_container_no_catalog.get(TaskManager)


@pytest.fixture(name="payload")
def payload_fixture(worker_type, task_manager):
    """Task payload fixture"""
    if worker_type == "cpu":
        payload_class = TestTaskPayload
    else:
        payload_class = TestIOTaskPayload
    return payload_class(
        user_id=task_manager.user.id,
        catalog_id=DEFAULT_CATALOG_ID,
    )


@pytest.fixture(name="parent_payload")
def parent_payload_fixture(worker_type, task_manager):
    """Parent task payload fixture"""
    if worker_type == "cpu":
        payload_class = TestTaskPayload
    else:
        payload_class = TestIOTaskPayload
    return payload_class(
        user_id=task_manager.user.id,
        catalog_id=DEFAULT_CATALOG_ID,
        run_child_task=True,
    )


@pytest.mark.asyncio
async def test_submit_task(task_manager, payload):
    """Test task manager service"""
    task_id = await task_manager.submit(payload=payload)
    task = await wait_for_async_task(task_manager, task_id)
    assert task.status == "SUCCESS"
    assert task.progress == {"percent": 100}
    assert isinstance(task.start_time, datetime.datetime)
    assert isinstance(task.date_done, datetime.datetime)

    # check task progress history
    progress_history = task.progress_history
    assert progress_history == ProgressHistory(
        data=[
            LogMessage(percent=0, message=None, timestamp=progress_history.data[0].timestamp),
            LogMessage(percent=0, message=None, timestamp=progress_history.data[1].timestamp),
            LogMessage(percent=20, message=None, timestamp=progress_history.data[2].timestamp),
            LogMessage(percent=40, message=None, timestamp=progress_history.data[3].timestamp),
            LogMessage(percent=60, message=None, timestamp=progress_history.data[4].timestamp),
            LogMessage(percent=80, message=None, timestamp=progress_history.data[5].timestamp),
            LogMessage(percent=100, message=None, timestamp=progress_history.data[6].timestamp),
        ],
        compress_at=0,
    )

    # ensure task is in the tasks list
    tasks, _ = await task_manager.list_tasks()
    matching_tasks = [task for task in tasks if str(task.id) == task_id]
    assert len(matching_tasks) == 1

    # check test task result
    task_result = await task_manager.get_task_result(task_id)
    assert task_result == "Test task result"


@pytest.mark.parametrize("worker_type", ["cpu"], indirect=True)
@pytest.mark.asyncio
async def test_schedule_interval_task(task_manager, payload):
    """Test task manager service"""
    periodic_task_id = await task_manager.schedule_interval_task(
        name="Run test task every 1 second",
        payload=payload,
        interval=Interval(every=1, period="seconds"),
    )
    # wait for 5 seconds
    await asyncio.sleep(10)

    # check if task is running
    periodic_task = await task_manager.get_periodic_task(periodic_task_id)
    assert periodic_task.total_run_count > 2

    # delete task
    await task_manager.delete_periodic_task(periodic_task_id)
    with pytest.raises(DocumentNotFoundError):
        await task_manager.get_periodic_task(periodic_task_id)


@pytest.mark.parametrize("worker_type", ["cpu"], indirect=True)
@pytest.mark.parametrize("timezone", [None, "Asia/Singapore"])
@pytest.mark.asyncio
async def test_schedule_cron_task(task_manager, payload, timezone):
    """Test task manager service"""
    sg_time = datetime.datetime.now(pytz.timezone("Asia/Singapore"))
    periodic_task_id = await task_manager.schedule_cron_task(
        name="Run test task for specific hours",
        payload=payload,
        crontab=Crontab(
            minute="*",
            # specify 2 hours to run the task to ensure it runs at least once in the test
            # even if the test is run at the end of the hour
            hour=f"{sg_time.hour}, {(sg_time.hour + 1) % 24}",
            day_of_week="*",
            day_of_month="*",
            month_of_year="*",
        ),
        timezone=timezone,
    )

    # wait for 1.5 minute
    await asyncio.sleep(90)

    # check if task is running
    periodic_task = await task_manager.get_periodic_task(periodic_task_id)
    if timezone:
        # if timezone is specified, the task will run in the specified timezone
        assert periodic_task.total_run_count >= 1
    else:
        # if no timezone is specified, the task will run in UTC (expect no run)
        assert periodic_task.total_run_count == 0

    # delete task
    await task_manager.delete_periodic_task(periodic_task_id)
    with pytest.raises(DocumentNotFoundError):
        await task_manager.get_periodic_task(periodic_task_id)


@pytest.mark.parametrize("worker_type", ["cpu"], indirect=True)
@pytest.mark.asyncio
async def test_revoke_task(task_manager, persistent):
    """Test task manager revoke task"""
    payload = TestTaskPayload(
        user_id=task_manager.user.id,
        catalog_id=DEFAULT_CATALOG_ID,
        sleep=1,
    )
    task_id = await task_manager.submit(payload=payload)
    start_time = time.time()
    while (time.time() - start_time) < 20:
        task = await task_manager.get_task(task_id)
        if (
            task.status == TaskStatus.STARTED
            and task.progress
            and task.progress.get("percent", 0) > 0
        ):
            break
        await asyncio.sleep(0.1)
    await task_manager.revoke_task(task_id)
    task = await task_manager.get_task(task_id)
    assert task.status == TaskStatus.REVOKED


@pytest.mark.asyncio
async def test_submit_child_task(task_manager, parent_payload, celery_service):
    """Test submitting child task from parent task"""
    persistent, _ = celery_service
    task_id = await task_manager.submit(payload=parent_payload)
    task = await wait_for_async_task(task_manager, task_id)
    assert task.status == "SUCCESS"
    assert task.progress == {"percent": 100}

    # check child task tracked in parent task result
    task_document = await persistent.find_one(
        collection_name=Task.collection_name(),
        query_filter={"_id": task_id},
    )
    assert len(task_document["child_task_ids"]) == 1
    child_task_id = task_document["child_task_ids"][0]

    # check parent task tracked in child task result
    task_document = await persistent.find_one(
        collection_name=Task.collection_name(),
        query_filter={"_id": child_task_id},
    )
    assert task_document["parent_id"] == str(task_id)
