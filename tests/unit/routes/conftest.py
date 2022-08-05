"""
Fixture for API unit tests
"""
from __future__ import annotations

from contextlib import asynccontextmanager
from unittest.mock import patch

import pymongo
import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from fastapi.testclient import TestClient
from mongomock_motor import AsyncMongoMockClient

from featurebyte.app import app
from featurebyte.enum import CollectionName
from featurebyte.persistent import GitDB
from featurebyte.persistent.mongo import MongoDB


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
                CollectionName.EVENT_DATA: [
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
        gitdb.insert_doc_name_func(CollectionName.EVENT_DATA, lambda doc: doc["name"])
        yield gitdb


@pytest.fixture(scope="session")
def user_id():
    return ObjectId()


@pytest.fixture()
def test_api_client_persistent(persistent, user_id):
    """
    Test API client
    """
    with patch("featurebyte.app._get_persistent") as mock_get_persistent:
        with patch("featurebyte.app.User") as mock_user:
            mock_user.return_value.id = user_id
            mock_get_persistent.return_value = persistent
            with TestClient(app) as client:
                yield client, persistent
