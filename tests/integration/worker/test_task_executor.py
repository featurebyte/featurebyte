"""
Tests for task executor
"""
import datetime
from enum import Enum
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.worker.task.base import TASK_MAP, BaseTask
from tests.util.task import TaskExecutor


@pytest.fixture(name="command_class")
def command_class_fixture():
    class Command(str, Enum):
        """Command enum used for testing"""

        RANDOM_COMMAND = "random_command"

    return Command


@pytest.fixture(name="random_task_payload_class")
def random_task_payload_class_fixture(command_class):
    """RandomTaskPayload class"""

    class RandomTaskPayload(BaseTaskPayload):
        """RandomTaskPayload class"""

        output_collection_name = "random_collection"
        command = command_class.RANDOM_COMMAND

    return RandomTaskPayload


@pytest.fixture(name="random_task_class")
def random_task_class_fixture(random_task_payload_class, persistent):
    """RandomTask class"""

    class RandomTask(BaseTask):
        """RandomTask class"""

        payload_class = random_task_payload_class

        async def execute(self) -> None:
            """Run some task"""
            await persistent.insert_one(
                collection_name="random_collection",
                document={
                    "_id": self.payload.output_document_id,
                    "user_id": self.user.id,
                    "output_document_id": self.payload.output_document_id,
                },
                user_id=self.user.id,
            )

    yield RandomTask


def test_extend_base_task_payload(random_task_payload_class):
    """Test the property & dict method of the extended payload"""

    user_id = ObjectId()
    document_id = ObjectId()
    payload_obj = random_task_payload_class(
        user_id=user_id, catalog_id=DEFAULT_CATALOG_ID, output_document_id=document_id
    )
    assert payload_obj.dict() == {
        "command": "random_command",
        "user_id": user_id,
        "catalog_id": DEFAULT_CATALOG_ID,
        "output_document_id": document_id,
    }
    assert payload_obj.task_output_path == f"/random_collection/{document_id}"


@pytest.mark.asyncio
async def test_task_executor(random_task_class, persistent):
    """Test task get loaded properly when extending BaseTask & BaskTaskPayload"""
    # check task get loaded to TASK_MAP properly
    assert "random_command" in TASK_MAP
    assert TASK_MAP["random_command"] == random_task_class

    # run executor
    user_id = ObjectId()
    document_id = ObjectId()
    await TaskExecutor(
        payload={
            "command": "random_command",
            "user_id": user_id,
            "catalog_id": DEFAULT_CATALOG_ID,
            "output_document_id": document_id,
        },
        progress=None,
    ).execute()

    # check store
    document = await persistent.find_one("random_collection", {"user_id": user_id}, user_id=user_id)
    assert document == {
        "_id": document_id,
        "created_at": document["created_at"],
        "user_id": user_id,
        "output_document_id": document_id,
    }


def test_task_has_been_implemented(random_task_class, command_class):
    """
    Test implement a task whose command has been implemented before
    """
    # check task get loaded to TASK_MAP properly
    assert "random_command" in TASK_MAP
    assert TASK_MAP["random_command"] == random_task_class
    with pytest.raises(ValueError) as exc:

        class ConflictTaskPayload(BaseTaskPayload):
            """Payload which going to cause conflict"""

            command = command_class.RANDOM_COMMAND

        class ConflictTask(BaseTask):
            """RandomTask class"""

            payload_class = ConflictTaskPayload

        _ = ConflictTask

    assert 'Command "random_command" has been implemented.' in str(exc.value)

    # initiate BaseTask without override payload_class will trigger NotImplementedError
    with pytest.raises(NotImplementedError):
        BaseTask(
            payload={},
            progress=None,
            user=None,
            get_persistent=None,
            get_storage=None,
            get_temp_storage=None,
            get_credential=None,
        )
