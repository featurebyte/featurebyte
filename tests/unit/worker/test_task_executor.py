"""
Tests for task executor
"""

import datetime
from multiprocessing import Array, Process, Value
from uuid import uuid4

import gevent
import pytest
from bson import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.models.base import User
from featurebyte.schema.worker.task.base import TaskType
from featurebyte.worker import initialize_asyncio_event_loop
from featurebyte.worker.registry import TASK_REGISTRY_MAP
from featurebyte.worker.task_executor import TaskExecutor as WorkerTaskExecutor
from featurebyte.worker.task_executor import run_async
from featurebyte.worker.test_util.random_task import RandomTask, RandomTaskPayload, TestCommand


@pytest.fixture(name="random_task_class")
def random_task_class_fixture():
    """RandomTask class"""
    # Cannot reinitialize the same command
    if TestCommand.RANDOM_COMMAND in TASK_REGISTRY_MAP:
        yield TASK_REGISTRY_MAP[TestCommand.RANDOM_COMMAND]
    else:
        # Register the RANDOM_COMMAND in TASK_REGISTRY_MAP
        TASK_REGISTRY_MAP[TestCommand.RANDOM_COMMAND] = RandomTask
        yield RandomTask


def test_extend_base_task_payload():
    """Test the property & dict method of the extended payload"""

    user_id = ObjectId()
    document_id = ObjectId()
    payload_obj = RandomTaskPayload(
        user_id=user_id, catalog_id=DEFAULT_CATALOG_ID, output_document_id=document_id
    )
    assert payload_obj.model_dump() == {
        "command": "random_command",
        "user_id": user_id,
        "catalog_id": DEFAULT_CATALOG_ID,
        "output_document_id": document_id,
        "output_collection_name": "random_collection",
        "task_type": TaskType.IO_TASK,
        "priority": 2,
        "is_scheduled_task": False,
        "is_revocable": False,
        "is_rerunnable": False,
    }
    assert payload_obj.task_output_path == f"/random_collection/{document_id}"


class TaskExecutor(WorkerTaskExecutor):
    """TaskExecutor class"""

    command_type = str


@pytest.mark.asyncio
async def test_task_executor(random_task_class, persistent, app_container):
    """Test task get loaded properly when extending BaseTask & BaskTaskPayload"""
    _ = random_task_class

    # check task get loaded to TASK_REGISTRY_MAP properly
    assert TestCommand.RANDOM_COMMAND in TASK_REGISTRY_MAP
    assert TASK_REGISTRY_MAP[TestCommand.RANDOM_COMMAND] == random_task_class

    # add task record
    task_id = app_container.task_id
    await persistent._db["celery_taskmeta"].insert_one(
        document={"_id": str(task_id)},
    )

    # run executor
    user_id = ObjectId()
    document_id = ObjectId()
    app_container.override_instances_for_test({
        "persistent": persistent,
        "user": User(id=user_id),
    })
    await TaskExecutor(
        payload={
            "command": "random_command",
            "user_id": user_id,
            "catalog_id": DEFAULT_CATALOG_ID,
            "output_document_id": document_id,
        },
        task_id=task_id,
        app_container=app_container,
    ).execute()

    # check store
    document = await persistent.find_one("random_collection", {"user_id": user_id})
    assert document == {
        "_id": document_id,
        "created_at": document["created_at"],
        "user_id": user_id,
        "output_document_id": document_id,
    }

    # check task start time and description is updated
    document = await persistent.find_one(
        collection_name="celery_taskmeta", query_filter={"_id": str(task_id)}
    )
    assert isinstance(document["start_time"], datetime.datetime)
    assert document["description"] == "Execute random task"

    # check that progress is not overridden
    assert document["progress"] == {
        "percent": 100,
        "message": "Random task completed.",
        "metadata": {"some_key": "some_value"},
    }

    # check get task result
    task_manager = app_container.task_manager
    task_result = await task_manager.get_task_result(task_id=str(task_id))
    assert task_result == 1234


def run_process_task(state: Value, exception_value: Value, timeout: int):
    """Run task in a separate process using greenlet thread"""
    from gevent import monkey

    # all imports should be done after monkey patch
    monkey.patch_all()
    import time

    initialize_asyncio_event_loop()

    async def async_task(state: Value):
        """Async task that blocks for 2 seconds and update state to 2"""
        time.sleep(2)
        state.value = 2

    def run_greenlet_task():
        """Run task in a separate greenlet"""
        try:
            run_async(coro=async_task(state), request_id=uuid4(), timeout=timeout)
        except Exception as exc:
            error_message = str(exc).encode("utf-8")
            for idx, byte in enumerate(error_message[:100]):
                exception_value[idx] = byte

    # execute multiple tasks in greenlet thread
    gevent.joinall([gevent.spawn(run_greenlet_task), gevent.spawn(run_greenlet_task)])


@pytest.mark.parametrize("timeout", [10, 1])
def test_run_async(timeout):
    """
    Test run async task in a separate thread
    """
    state = Value("i", 1)
    exception_value = Array("c", 100)
    process = Process(target=run_process_task, args=(state, exception_value, timeout))

    if timeout > 2:
        process.start()
        process.join()
        # state should be updated by async task in greenlet thread
        assert state.value == 2
        output = exception_value[:].decode("utf-8").strip("\x00")
        assert output == "", output
    else:
        # expect celery SoftTimeLimitExceeded error
        # with pytest.raises(SoftTimeLimitExceeded) as exc:
        process.start()
        process.join()
        # state should remain unchanged
        assert state.value == 1
        output = exception_value[:].decode("utf-8").strip("\x00")
        assert output == f"SoftTimeLimitExceeded('Task timed out after {timeout}s',)", output
