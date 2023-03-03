"""
Tests for process store
"""
from typing import Any

import asyncio
import subprocess
import threading
import time

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.schema.task import TaskId
from featurebyte.schema.worker.task.test import TestTaskPayload
from featurebyte.service.task_manager import TaskManager


class RunThread(threading.Thread):
    """
    Thread to execute query
    """

    def __init__(self, stdout: Any) -> None:
        self.stdout = stdout
        super().__init__()

    def run(self) -> None:
        """
        Run async function
        """
        print(self.stdout.read().decode("utf-8"))


@pytest.fixture(scope="session", name="celery_service")
def celery_service_fixture():
    """
    Start celery service for testing
    """
    proc = subprocess.Popen(
        [
            "celery",
            "--app",
            "featurebyte.worker.start.celery",
            "worker",
            "--loglevel=debug",
            # "--beat", "--scheduler", "celerybeatmongo.schedulers.MongoScheduler"
        ],
        stdout=subprocess.PIPE,
    )
    thread = RunThread(proc.stdout)
    thread.daemon = True
    thread.start()
    yield proc
    proc.terminate()


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


@pytest.mark.asyncio
async def test_task_manager(celery_service):
    """Test task manager service"""
    user_id = ObjectId()
    task_manager = TaskManager(user_id=user_id)
    payload = TestTaskPayload(
        user_id=user_id,
        workspace_id=DEFAULT_WORKSPACE_ID,
    )
    task_id = await task_manager.submit(payload=payload)
    task = await wait_for_async_task(task_manager, task_id)
    assert task.status == "SUCCESS"
