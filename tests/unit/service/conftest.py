"""
Fixture for Service related unit tests
"""
from __future__ import annotations

from typing import AsyncIterator

from contextlib import asynccontextmanager
from unittest.mock import Mock, patch

import pymongo
import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from mongomock_motor import AsyncMongoMockClient

from featurebyte.models.event_data import EventDataModel
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
def user():
    """
    Mock user
    """
    user = Mock()
    user.id = ObjectId()
    return user
