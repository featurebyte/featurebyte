"""
Test for task manager service
"""
import time
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.process_store import ProcessStore
from featurebyte.worker.progress import GlobalProgress
from tests.util.task import Command, LongRunningPayload, TaskExecutor

ProcessStore._command_class = Command
ProcessStore._task_executor = TaskExecutor


@pytest.fixture(name="user_id")
def user_id_fixture():
    """User ID fixture"""
    return ObjectId()


@pytest.fixture(name="task_manager")
def task_manager_fixture(user_id):
    """Task manager fixture"""
    with patch("featurebyte.service.task_manager.ProcessStore", wraps=ProcessStore):
        return TaskManager(user_id=user_id)


def test_task_manager__long_running_tasks(task_manager, user_id):
    """Test task manager service"""
    expected_task_statuses = []
    for _ in range(3):
        task_status_id = task_manager.submit(payload=LongRunningPayload(user_id=user_id))
        task_status = task_manager.get_task_status(task_status_id=task_status_id)
        assert task_status.id == task_status_id
        assert task_status.status == "running"
        expected_task_statuses.append(task_status)
        task_statuses, _ = task_manager.list_task_status()
        assert task_statuses == expected_task_statuses

    # check progress update
    for task_status in expected_task_statuses:
        progress_queue = GlobalProgress.get_progress(user_id=user_id, task_status_id=task_status.id)
        progress_percents = [progress_queue.get()["percent"]]
        while progress_percents[-1] < 100:
            progress_percents.append(progress_queue.get()["percent"])
        assert progress_queue.empty()
        assert progress_percents == [10 * (i + 1) for i in range(10)]

    # wait a bit to let the process finishes
    time.sleep(0.1)

    # check all task completed
    task_statuses, _ = task_manager.list_task_status()
    for task_status in task_statuses:
        assert task_status.status == "complete"


def test_task_manager__not_found_task(task_manager, user_id):
    """Test task manager service on not found task"""

    class NewTaskPlayload(BaseTaskPayload):
        collection_name = "random_collection"
        command = Command.UNKNOWN_TASK_COMMAND

    task_status_id = task_manager.submit(payload=NewTaskPlayload(user_id=user_id))

    # wait until task finishes
    time.sleep(1)

    task_status = task_manager.get_task_status(task_status_id=task_status_id)
    assert task_status.status == "error"

    # test retrieve random task_status_id
    task_status_random = task_manager.get_task_status(task_status_id=ObjectId())
    assert task_status_random is None
