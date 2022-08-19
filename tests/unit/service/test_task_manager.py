"""
Test for task manager service
"""
import math
import time
from unittest.mock import Mock, patch

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
        assert task_status.status == "STARTED"
        expected_task_statuses.append(task_status)
        task_statuses, _ = task_manager.list_task_status()
        assert task_statuses == expected_task_statuses

    # check progress update
    for task_status in expected_task_statuses:
        progress_queue = GlobalProgress().get_progress(
            user_id=user_id, task_status_id=task_status.id
        )
        progress_percents = [progress_queue.get()["percent"]]
        while progress_percents[-1] < 100:
            progress_percents.append(progress_queue.get()["percent"])
        assert progress_queue.empty()
        assert progress_percents == [10 * (i + 1) for i in range(10)]

    # wait a bit to let the process finishes
    for task_status in task_statuses:
        process = ProcessStore().get(user_id, task_status.id)
        process.join()

    # check all task completed
    task_statuses, _ = task_manager.list_task_status()
    for task_status in task_statuses:
        assert task_status.status == "SUCCESS"


def test_task_manager__not_found_task(task_manager, user_id):
    """Test task manager service on not found task"""

    class NewTaskPlayload(BaseTaskPayload):
        output_collection_name = "random_collection"
        command = Command.UNKNOWN_TASK_COMMAND

    task_status_id = task_manager.submit(payload=NewTaskPlayload(user_id=user_id))

    # wait until task finishes
    time.sleep(1)

    task_status = task_manager.get_task_status(task_status_id=task_status_id)
    assert task_status.status == "FAILURE"

    # test retrieve random task_status_id
    task_status_random = task_manager.get_task_status(task_status_id=ObjectId())
    assert task_status_random is None


@patch("featurebyte.service.task_manager.ProcessStore", wraps=ProcessStore)
def test_task_manager__list_task_status(mock_process_store):
    """Test task manager service -- list task status"""
    _ = mock_process_store
    user_id = ObjectId()
    task_manager = TaskManager(user_id=user_id)

    task_num = 10
    task_status_ids = []
    for _ in range(task_num):
        task_status_ids.append(task_manager.submit(payload=LongRunningPayload(user_id=user_id)))

    page_sizes = [1, 2, 5, 10, 20]
    for page_size in page_sizes:
        total_page = math.ceil(task_num / page_size)
        ascending_list = []
        descending_list = []
        for page in range(1, total_page + 1):
            items, total = task_manager.list_task_status(
                page=page, page_size=page_size, ascending=True
            )
            assert total == task_num
            ascending_list.extend(items)

            items, total = task_manager.list_task_status(
                page=page, page_size=page_size, ascending=False
            )
            assert total == task_num
            descending_list.extend(items)

        # check list order
        assert [item.id for item in ascending_list] == sorted(task_status_ids)
        assert [item.id for item in descending_list] == sorted(task_status_ids, reverse=True)
