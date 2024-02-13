"""
Tests for process store
"""
import asyncio
import datetime
import time

import pytest

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.periodic_task import Interval
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


@pytest.mark.asyncio
async def test_submit_task(task_manager, payload):
    """Test task manager service"""
    task_id = await task_manager.submit(payload=payload)
    task = await wait_for_async_task(task_manager, task_id)
    assert task.status == "SUCCESS"
    assert task.progress == {"percent": 100}
    assert isinstance(task.start_time, datetime.datetime)
    assert isinstance(task.date_done, datetime.datetime)

    # ensure task is in the tasks list
    tasks, _ = await task_manager.list_tasks()
    matching_tasks = [task for task in tasks if str(task.id) == task_id]
    assert len(matching_tasks) == 1


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
