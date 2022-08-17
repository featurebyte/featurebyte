"""
Tests for task executor
"""
from enum import Enum

import pytest
from bson.objectid import ObjectId

from featurebyte.worker.task.base import TASK_MAP, BaseTask, BaseTaskPayload
from featurebyte.worker.task_executor import TaskExecutor


@pytest.fixture(name="random_task_payload_class")
def random_task_payload_class_fixture():
    """RandomTaskPayload class"""

    class Command(str, Enum):

        RANDOM_COMMAND = "random_command"

    class RandomTaskPayload(BaseTaskPayload):

        collection_name = "random_collection"
        command = Command.RANDOM_COMMAND

    return RandomTaskPayload


def test_extend_base_task_payload(random_task_payload_class):
    """Test the property & dict method of the extended payload"""

    user_id = ObjectId()
    document_id = ObjectId()
    payload_obj = random_task_payload_class(user_id=user_id, document_id=document_id)
    assert payload_obj.dict() == {
        "command": "random_command",
        "user_id": user_id,
        "document_id": document_id,
    }
    assert payload_obj.task_output_path == f"random_collection/{document_id}"


def test_task_executor(random_task_payload_class):
    """Test task get loaded properly when extending BaseTask & BaskTaskPayload"""
    store = {}

    class RandomTask(BaseTask):

        payload_class = random_task_payload_class
        command = random_task_payload_class.command

        def execute(self) -> None:
            """Run some task"""
            store["new_item"] = {
                "user_id": self.payload.user_id,
                "document_id": self.payload.document_id,
            }

    # check task get loaded to TASK_MAP properly
    assert "random_command" in TASK_MAP
    assert TASK_MAP["random_command"] == RandomTask

    # check store before running executor
    assert store == {}

    # run executor
    user_id = ObjectId()
    document_id = ObjectId()
    TaskExecutor(
        payload={
            "command": "random_command",
            "user_id": user_id,
            "document_id": document_id,
        }
    )

    # check store
    assert store == {"new_item": {"user_id": user_id, "document_id": document_id}}
