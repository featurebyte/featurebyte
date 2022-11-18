"""
Fixture for API unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

import json
import traceback
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId
from fastapi.testclient import TestClient

from featurebyte.app import app
from featurebyte.enum import WorkerCommand
from featurebyte.schema.task import TaskStatus
from featurebyte.utils.credential import get_credential
from featurebyte.worker.task.base import TASK_MAP


@pytest.fixture(scope="session")
def user_id():
    """
    Mock user id
    """
    return ObjectId()


@pytest.fixture(name="mock_get_session", autouse=True)
def get_mock_get_session_fixture():
    """
    Returns a mocked get_feature_store_session.
    """
    with patch(
        "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
    ) as mocked_get_session:
        yield mocked_get_session


@pytest.fixture(autouse=True, scope="function")
def mock_process_store(request, persistent, storage, temp_storage):
    """
    Mock process store to avoid running task in subprocess for testing
    """
    if "no_mock_process_store" in request.keywords:
        yield
        return

    with patch("featurebyte.service.task_manager.ProcessStore.submit") as mock_submit:
        process_store = {}

        async def submit(payload, output_path):
            _ = output_path
            payload_dict = json.loads(payload)
            command = WorkerCommand(payload_dict["command"])
            task = TASK_MAP[command](
                payload=payload_dict,
                progress=Mock(),
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

            task_id = ObjectId()
            process_store[task_id] = {
                "output_path": output_path,
                "payload": payload_dict,
                "status": status,
                "traceback": traceback_info,
            }
            return task_id

        mock_submit.side_effect = submit

        with patch("featurebyte.service.task_manager.ProcessStore.get") as mock_get:

            async def get(user_id, task_id):
                _ = user_id
                process_data = process_store.get(task_id, {})
                return {
                    "id": task_id,
                    "process": Mock(exitcode=0),
                    "output_path": process_data.get("output_path", "some_path"),
                    "payload": process_data.get("payload", {"key": "value"}),
                    "status": process_data.get("status", TaskStatus.SUCCESS),
                    "traceback": process_data.get("traceback"),
                }

            mock_get.side_effect = get
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
