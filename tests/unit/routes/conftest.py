"""
Fixture for API unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

import datetime
import traceback
from unittest.mock import Mock, patch
from uuid import uuid4

import pytest
from bson.objectid import ObjectId
from fastapi.testclient import TestClient

from featurebyte.app import app
from featurebyte.models.base import User
from featurebyte.models.task import Task as TaskModel
from featurebyte.schema.task import TaskStatus
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.utils.credential import get_credential
from featurebyte.worker.task.base import TASK_MAP


@pytest.fixture(scope="session")
def user_id():
    """
    Mock user id
    """
    return ObjectId()


@pytest.fixture(name="mock_get_session", autouse=True)
def get_mock_get_session_fixture(session_manager, snowflake_execute_query):
    """
    Returns a mocked get_feature_store_session.
    """
    _, _ = session_manager, snowflake_execute_query
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
    ) as mocked_get_session:
        yield mocked_get_session


@pytest.fixture(autouse=True, scope="function")
def mock_task_manager(persistent, storage, temp_storage):
    """
    Mock celery task manager for testing
    """
    task_status = {}
    with patch("featurebyte.service.task_manager.get_persistent") as mock_get_persistent:
        mock_get_persistent.return_value = persistent
        with patch("featurebyte.service.task_manager.TaskManager.submit") as mock_submit:

            async def submit(payload: BaseTaskPayload):
                kwargs = payload.json_dict()
                kwargs["task_output_path"] = payload.task_output_path
                task = TASK_MAP[payload.command](
                    payload=kwargs,
                    progress=Mock(),
                    user=User(id=kwargs.get("user_id")),
                    get_credential=get_credential,
                    get_persistent=lambda: persistent,
                    get_storage=lambda: storage,
                    get_temp_storage=lambda: temp_storage,
                )
                try:
                    await task.execute()
                    status = TaskStatus.SUCCESS
                    traceback_info = None
                except Exception:  # pylint: disable=broad-except
                    status = TaskStatus.FAILURE
                    traceback_info = traceback.format_exc()

                task_id = str(uuid4())
                task_status[task_id] = status

                # insert task into db manually since we are mocking celery
                task = TaskModel(
                    _id=task_id,
                    status=status,
                    result="",
                    children=[],
                    date_done=datetime.datetime.utcnow(),
                    name=payload.command,
                    args=[],
                    kwargs=kwargs,
                    worker="worker",
                    retries=0,
                    queue="default",
                    traceback=traceback_info,
                )
                document = task.dict(by_alias=True)
                document["_id"] = str(document["_id"])
                await persistent._db[TaskModel.collection_name()].insert_one(document)
                return task_id

            mock_submit.side_effect = submit

            with patch("featurebyte.service.task_manager.celery") as mock_celery:

                def get_task(task_id):
                    status = task_status.get(task_id)
                    if status is None:
                        return None
                    return Mock(status=status)

                mock_celery.AsyncResult.side_effect = get_task
                yield


@pytest.fixture()
def test_api_client_persistent(persistent, user_id, temp_storage):
    """
    Test API client
    """
    with patch("featurebyte.app.get_persistent") as mock_get_persistent:
        with patch("featurebyte.app.get_temp_storage") as mock_get_temp_storage:
            with patch("featurebyte.app.User") as mock_user:
                mock_user.return_value.id = user_id
                mock_get_persistent.return_value = persistent
                mock_get_temp_storage.return_value = temp_storage
                with TestClient(app) as client:
                    yield client, persistent


@pytest.fixture(name="get_credential")
def get_credential_fixture(config):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return config.credentials.get(feature_store_name)

    return get_credential
