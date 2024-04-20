"""
Tests for task executor
"""

import datetime
from uuid import uuid4

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.schema.worker.task.base import TaskType
from featurebyte.worker.registry import TASK_REGISTRY_MAP
from featurebyte.worker.task_executor import TaskExecutor as WorkerTaskExecutor
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
    assert payload_obj.dict() == {
        "command": "random_command",
        "user_id": user_id,
        "catalog_id": DEFAULT_CATALOG_ID,
        "output_document_id": document_id,
        "output_collection_name": "random_collection",
        "task_type": TaskType.IO_TASK,
        "priority": 2,
        "is_scheduled_task": False,
        "is_revocable": False,
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
    task_id = uuid4()
    await persistent._db["celery_taskmeta"].insert_one(
        document={"_id": str(task_id)},
    )

    # run executor
    user_id = ObjectId()
    document_id = ObjectId()
    app_container.override_instances_for_test(
        {
            "persistent": persistent,
            "user": User(id=user_id),
        }
    )
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

    # check get task result
    task_manager = app_container.task_manager
    task_result = await task_manager.get_task_result(task_id=str(task_id))
    assert task_result == 1234
