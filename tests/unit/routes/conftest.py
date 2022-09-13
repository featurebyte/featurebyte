"""
Fixture for API unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import AsyncIterator

import json
from contextlib import asynccontextmanager
from unittest.mock import Mock, patch

import pymongo
import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from featurebyte.app import app
from featurebyte.enum import WorkerCommand
from featurebyte.models.event_data import EventDataModel
from featurebyte.persistent import GitDB
from featurebyte.persistent.mongo import MongoDB
from featurebyte.utils.credential import get_credential
from featurebyte.worker.task.base import TASK_MAP


def pytest_generate_tests(metafunc):
    """
    Parametrize persistent fixture
    """
    if "persistent" in metafunc.fixturenames:
        metafunc.parametrize("persistent", ["gitdb", "mongodb"], indirect=True)


@pytest_asyncio.fixture(name="persistent")
async def persistent_fixture(request):
    """
    Persistent fixture
    """
    if request.param == "mongodb":
        with patch("motor.motor_asyncio.AsyncIOMotorClient.__new__") as mock_new:
            mongo_client = AsyncMongoMockClient()
            mock_new.return_value = mongo_client
            persistent = MongoDB(uri="mongodb://server.example.com:27017", database="test")
            database = mongo_client["test"]
            collection_index_map = {
                EventDataModel.collection_name(): [
                    ("_id", {}),
                    ("user_id", {}),
                    ("source", {}),
                    (
                        [("user_id", pymongo.ASCENDING), ("name", pymongo.ASCENDING)],
                        {"unique": True},
                    ),
                    ([("name", pymongo.TEXT), ("source.type", pymongo.TEXT)], {}),
                ]
            }
            for collection_name, create_index_param_list in collection_index_map.items():
                for param, param_kwargs in create_index_param_list:
                    await database[collection_name].create_index(param, **param_kwargs)

            # skip session in unit tests
            @asynccontextmanager
            async def start_transaction() -> AsyncIterator[MongoDB]:
                yield persistent

            with patch.object(persistent, "start_transaction", start_transaction):
                yield persistent

    if request.param == "gitdb":
        gitdb = GitDB()
        yield gitdb


@pytest.fixture(scope="session")
def user_id():
    """
    Mock user id
    """
    return ObjectId()


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
            await task.execute()
            task_id = ObjectId()
            process_store[task_id] = {
                "output_path": output_path,
                "payload": payload_dict,
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
                    "status": "SUCCESS",
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
