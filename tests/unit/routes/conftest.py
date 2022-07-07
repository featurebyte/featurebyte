"""
Fixture for API unit tests
"""
from __future__ import annotations

from unittest.mock import patch

import mongomock
import pymongo
import pytest
from fastapi.testclient import TestClient

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


@pytest.fixture(name="persistent")
def persistent_fixture(request):
    """
    Persistent fixture
    """
    if request.param == "mongodb":
        with mongomock.patch(servers=(("server.example.com", 27017),)):
            persistent = MongoDB(uri="mongodb://server.example.com:27017", database="test")
            mongo_client = pymongo.MongoClient("mongodb://server.example.com:27017")
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
                    database[collection_name].create_index(param, **param_kwargs)
            yield persistent

    if request.param == "gitdb":
        gitdb = GitDB()
        gitdb.insert_doc_name_func(CollectionName.EVENT_DATA, lambda doc: doc["name"])
        yield gitdb


@pytest.fixture()
def test_api_client(persistent):
    """
    Test API client
    """
    with patch("featurebyte.app._get_persistent") as mock_get_persistent:
        mock_get_persistent.return_value = persistent
        with TestClient(app) as client:
            yield client
