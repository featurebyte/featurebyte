"""
Tests for process store
"""
import asyncio
import datetime
import time

import pytest
from bson.objectid import ObjectId

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.periodic_task import Interval
from featurebyte.schema.task import TaskId
from featurebyte.schema.worker.task.test import TestTaskPayload
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
        if task.status in ["SUCCESS", "FAILURE"]:
            return task
        await asyncio.sleep(1)
    raise TimeoutError("Timeout waiting for task to finish")


@pytest.fixture(name="task_manager")
def task_manager_fixture(celery_service):
    """Task manager fixture"""
    persistent, celery = celery_service
    return TaskManager(
        user=User(id=ObjectId()),
        persistent=persistent,
        celery=celery,
        catalog_id=DEFAULT_CATALOG_ID,
    )


@pytest.mark.asyncio
async def test_submit_task(task_manager):
    """Test task manager service"""
    payload = TestTaskPayload(
        user_id=task_manager.user.id,
        catalog_id=DEFAULT_CATALOG_ID,
    )
    task_id = await task_manager.submit(payload=payload)
    task = await wait_for_async_task(task_manager, task_id)
    assert task.status == "SUCCESS"
    assert task.progress == {"percent": 100}
    assert isinstance(task.start_time, datetime.datetime)
    assert isinstance(task.date_done, datetime.datetime)

    # ensure task is in the tasks list
    tasks, total = await task_manager.list_tasks()
    assert total == 1
    assert str(tasks[0].id) == task_id


@pytest.mark.asyncio
async def test_schedule_interval_task(task_manager):
    """Test task manager service"""
    payload = TestTaskPayload(
        user_id=task_manager.user.id,
        catalog_id=DEFAULT_CATALOG_ID,
    )
    task_id = await task_manager.schedule_interval_task(
        name="Run test task every 1 second",
        payload=payload,
        interval=Interval(every=1, period="seconds"),
    )
    # wait for 5 seconds
    await asyncio.sleep(20)

    # check if task is running
    periodic_task = await task_manager.get_periodic_task(task_id)
    assert periodic_task.total_run_count > 2

    # delete task
    await task_manager.delete_periodic_task(task_id)
    with pytest.raises(DocumentNotFoundError):
        await task_manager.get_periodic_task(task_id)
