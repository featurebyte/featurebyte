"""
Test for task manager service
"""
import math
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


@pytest.mark.asyncio
async def test_task_manager__long_running_tasks(task_manager, user_id):
    """Test task manager service"""
    expected_tasks = []
    tasks = []
    for _ in range(3):
        task_id = await task_manager.submit(payload=LongRunningPayload(user_id=user_id))
        task = await task_manager.get_task(task_id=task_id)
        assert task.id == task_id
        assert task.status == "STARTED"
        expected_tasks.append(task)
        tasks, _ = await task_manager.list_tasks()
        assert tasks == expected_tasks

    # check progress update
    for task in expected_tasks:
        progress_queue = GlobalProgress().get_progress(user_id=user_id, task_id=task.id)
        progress_percents = [progress_queue.get()["percent"]]
        while progress_percents[-1] < 100:
            progress_percents.append(progress_queue.get()["percent"])
        assert progress_queue.empty()
        assert progress_percents == [10 * (i + 1) for i in range(10)]

    # wait a bit to let the process finishes
    for task in tasks:
        process_data = await ProcessStore().get(user_id, task.id)
        process_data["process"].join()

    # check all task completed
    tasks, _ = await task_manager.list_tasks()
    for task in tasks:
        assert task.status == "SUCCESS"


@pytest.mark.asyncio
async def test_task_manager__not_found_task(task_manager, user_id):
    """Test task manager service on not found task"""

    class NewTaskPayload(BaseTaskPayload):
        """NewTaskPayload class"""

        output_collection_name = "random_collection"
        command = Command.UNKNOWN_TASK_COMMAND

    task_id = await task_manager.submit(payload=NewTaskPayload(user_id=user_id))

    # wait until task finishes
    process_data = await ProcessStore().get(user_id, task_id)
    process_data["process"].join()

    task = await task_manager.get_task(task_id=task_id)
    assert task.status == "FAILURE"

    # test retrieve random task_status_id
    task_status_random = await task_manager.get_task(task_id=ObjectId())
    assert task_status_random is None


@patch("featurebyte.service.task_manager.ProcessStore", wraps=ProcessStore)
@pytest.mark.asyncio
async def test_task_manager__list_tasks(mock_process_store):
    """Test task manager service -- list task status"""
    _ = mock_process_store
    user_id = ObjectId()
    task_manager = TaskManager(user_id=user_id)

    task_num = 10
    task_ids = []
    for _ in range(task_num):
        task_ids.append(await task_manager.submit(payload=LongRunningPayload(user_id=user_id)))

    page_sizes = [1, 2, 5, 10, 20]
    for page_size in page_sizes:
        total_page = math.ceil(task_num / page_size)
        ascending_list = []
        descending_list = []
        for page in range(1, total_page + 1):
            items, total = await task_manager.list_tasks(
                page=page, page_size=page_size, ascending=True
            )
            assert total == task_num
            ascending_list.extend(items)

            items, total = await task_manager.list_tasks(
                page=page, page_size=page_size, ascending=False
            )
            assert total == task_num
            descending_list.extend(items)

        # check list order
        assert [item.id for item in ascending_list] == sorted(task_ids)
        assert [item.id for item in descending_list] == sorted(task_ids, reverse=True)
