"""
Fixture for Service related unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import AsyncIterator

import asyncio
import json
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
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.event_data import EventDataCreate
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_store import FeatureStoreService


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


@pytest.fixture(name="get_credential")
def get_credential_fixture(config):
    """
    get_credential fixture
    """

    async def get_credential(user_id, feature_store_name):
        _ = user_id
        return config.credentials.get(feature_store_name)

    return get_credential


@pytest.fixture(name="feature_store_service")
def feature_store_service_fixture(user, persistent):
    """FeatureStore service"""
    return FeatureStoreService(user=user, persistent=persistent)


@pytest.fixture(name="entity_service")
def entity_service_fixture(user, persistent):
    """Entity service"""
    return EntityService(user=user, persistent=persistent)


@pytest.fixture(name="event_data_service")
def event_data_service_fixture(user, persistent):
    """EventData service"""
    return EventDataService(user=user, persistent=persistent)


@pytest.fixture(name="feature_namespace_service")
def feature_namespace_service_fixture(persistent, user):
    """FeatureNamespaceService fixture"""
    return FeatureNamespaceService(persistent=persistent, user=user)


@pytest.fixture(name="feature_service")
def feature_service_fixture(persistent, user):
    """FeatureService fixture"""
    return FeatureService(persistent=persistent, user=user)


@pytest.fixture(name="feature_list_namespace_service")
def feature_list_namespace_service_fixture(persistent, user):
    """FeatureListNamespaceService fixture"""
    return FeatureListNamespaceService(persistent=persistent, user=user)


@pytest.fixture(name="feature_list_service")
def feature_list_service_fixture(persistent, user):
    """FeatureListService fixture"""
    return FeatureListService(persistent=persistent, user=user)


@pytest.fixture(name="feature_store")
def feature_store_fixture(feature_store_service, user):
    """FeatureStore model"""
    with open("tests/fixtures/request_payloads/feature_store.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_store = asyncio.run(
            feature_store_service.create_document(
                data=FeatureStoreCreate(**payload, user_id=user.id)
            )
        )
        return feature_store


@pytest.fixture(name="event_data")
def event_data_fixture(feature_store, event_data_service, user):
    """EventData model"""
    _ = feature_store
    with open("tests/fixtures/request_payloads/event_data.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        event_data = asyncio.run(
            event_data_service.create_document(data=EventDataCreate(**payload, user_id=user.id))
        )
        return event_data


@pytest.fixture(name="entity")
def entity_fixture(entity_service, user):
    """Entity model"""
    with open("tests/fixtures/request_payloads/entity.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        entity = asyncio.run(
            entity_service.create_document(data=EntityCreate(**payload, user_id=user.id))
        )
        return entity


@pytest.fixture(name="mock_insert_feature_registry")
def mock_insert_feature_registry_fixture():
    """
    Mock insert feature registry at the controller level
    """
    with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
        yield mock


@pytest.fixture(name="feature")
def feature_fixture(event_data, entity, feature_service, mock_insert_feature_registry):
    """Feature model"""
    _ = mock_insert_feature_registry
    with open("tests/fixtures/request_payloads/feature_sum_30m.json", encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = asyncio.run(feature_service.create_document(data=FeatureCreate(**payload)))
        return feature


@pytest.fixture(name="feature_list")
def feature_list_fixture(feature, feature_list_service):
    """Feature list model"""
    with open(
        "tests/fixtures/request_payloads/feature_list_single.json", encoding="utf"
    ) as fhandle:
        payload = json.loads(fhandle.read())
        feature_list = asyncio.run(
            feature_list_service.create_document(data=FeatureListCreate(**payload))
        )
        return feature_list
