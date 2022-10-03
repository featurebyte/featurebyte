"""
Fixture for Service related unit tests
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import AsyncIterator

import json
import os.path
from contextlib import asynccontextmanager
from unittest.mock import Mock, patch

import pymongo
import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from mongomock_motor import AsyncMongoMockClient

from featurebyte.models.event_data import EventDataModel
from featurebyte.persistent.git import GitDB
from featurebyte.persistent.mongo import MongoDB
from featurebyte.schema.entity import EntityCreate
from featurebyte.schema.event_data import EventDataCreate
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceServiceUpdate
from featurebyte.schema.feature_store import FeatureStoreCreate
from featurebyte.service.default_version_mode import DefaultVersionModeService
from featurebyte.service.deploy import DeployService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.feature_readiness import FeatureReadinessService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.online_enable import OnlineEnableService
from featurebyte.service.version import VersionService


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
def feature_namespace_service_fixture(user, persistent):
    """FeatureNamespaceService fixture"""
    return FeatureNamespaceService(user=user, persistent=persistent)


@pytest.fixture(name="feature_service")
def feature_service_fixture(user, persistent):
    """FeatureService fixture"""
    return FeatureService(user=user, persistent=persistent)


@pytest.fixture(name="feature_list_namespace_service")
def feature_list_namespace_service_fixture(user, persistent):
    """FeatureListNamespaceService fixture"""
    return FeatureListNamespaceService(user=user, persistent=persistent)


@pytest.fixture(name="feature_list_service")
def feature_list_service_fixture(user, persistent):
    """FeatureListService fixture"""
    return FeatureListService(user=user, persistent=persistent)


@pytest.fixture(name="feature_readiness_service")
def feature_readiness_service_fixture(user, persistent):
    """FeatureReadinessService fixture"""
    return FeatureReadinessService(user=user, persistent=persistent)


@pytest.fixture(name="default_version_mode_service")
def default_version_mode_service_fixture(user, persistent):
    """DefaultVersionModeService fixture"""
    return DefaultVersionModeService(user=user, persistent=persistent)


@pytest.fixture(name="online_enable_service")
def online_enable_service_fixture(user, persistent):
    """OnlineEnableService fixture"""
    return OnlineEnableService(user=user, persistent=persistent)


@pytest.fixture(name="version_service")
def version_service_fixture(user, persistent):
    """VersionService fixture"""
    return VersionService(user=user, persistent=persistent)


@pytest.fixture(name="deploy_service")
def deploy_service_fixture(user, persistent):
    """DeployService fixture"""
    return DeployService(user=user, persistent=persistent)


@pytest_asyncio.fixture(name="feature_store")
async def feature_store_fixture(test_dir, feature_store_service, user):
    """FeatureStore model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_store.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_store = await feature_store_service.create_document(
            data=FeatureStoreCreate(**payload, user_id=user.id)
        )
        return feature_store


@pytest_asyncio.fixture(name="event_data")
async def event_data_fixture(test_dir, feature_store, event_data_service, user):
    """EventData model"""
    _ = feature_store
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/event_data.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        event_data = await event_data_service.create_document(
            data=EventDataCreate(**payload, user_id=user.id)
        )
        return event_data


@pytest_asyncio.fixture(name="entity")
async def entity_fixture(test_dir, entity_service, user):
    """Entity model"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/entity.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        entity = await entity_service.create_document(data=EntityCreate(**payload, user_id=user.id))
        return entity


@pytest.fixture(name="mock_insert_feature_registry")
def mock_insert_feature_registry_fixture():
    """
    Mock insert feature registry at the controller level
    """
    with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
        yield mock


@pytest_asyncio.fixture(name="feature")
async def feature_fixture(
    test_dir, event_data, entity, feature_service, mock_insert_feature_registry
):
    """Feature model"""
    _ = event_data, entity, mock_insert_feature_registry
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature = await feature_service.create_document(data=FeatureCreate(**payload))
        return feature


@pytest_asyncio.fixture(name="production_ready_feature")
async def production_ready_feature_fixture(feature_readiness_service, feature):
    """Production ready readiness feature fixture"""
    prod_feat = await feature_readiness_service.update_feature(
        feature_id=feature.id, readiness="PRODUCTION_READY"
    )
    assert prod_feat.readiness == "PRODUCTION_READY"
    return prod_feat


@pytest_asyncio.fixture(name="feature_namespace")
async def feature_namespace_fixture(feature_namespace_service, feature):
    """FeatureNamespace fixture"""
    return await feature_namespace_service.get_document(document_id=feature.feature_namespace_id)


@pytest_asyncio.fixture(name="feature_list")
async def feature_list_fixture(test_dir, feature, feature_list_service):
    """Feature list model"""
    _ = feature
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_list_single.json")
    with open(fixture_path, encoding="utf") as fhandle:
        payload = json.loads(fhandle.read())
        feature_list = await feature_list_service.create_document(data=FeatureListCreate(**payload))
        return feature_list


@pytest_asyncio.fixture(name="feature_list_namespace")
async def feature_list_namespace_fixture(feature_list_namespace_service, feature_list):
    """FeatureListNamespace fixture"""
    return await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )


async def insert_feature_into_persistent(test_dir, user, persistent, readiness, name=None):
    """Insert a feature into persistent"""
    fixture_path = os.path.join(test_dir, "fixtures/request_payloads/feature_sum_30m.json")
    with open(fixture_path) as fhandle:
        payload = json.loads(fhandle.read())
        payload = FeatureCreate(**payload, user_id=user.id).dict(by_alias=True)
        payload["_id"] = ObjectId()
        payload["readiness"] = readiness
        if name:
            payload["name"] = name
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document=payload,
            user_id=user.id,
        )
        return feature_id


@pytest_asyncio.fixture(name="setup_for_feature_readiness")
async def setup_for_feature_readiness_fixture(
    test_dir,
    feature_list_service,
    feature_list_namespace_service,
    feature_namespace_service,
    feature,
    feature_list,
    user,
    persistent,
):
    """Setup for feature readiness test fixture"""
    namespace = await feature_namespace_service.get_document(
        document_id=feature.feature_namespace_id
    )
    assert namespace.default_version_mode == "AUTO"
    assert namespace.default_feature_id == feature.id
    assert namespace.feature_ids == [feature.id]

    # add a deprecated feature version first
    new_feature_id = await insert_feature_into_persistent(
        test_dir=test_dir,
        user=user,
        persistent=persistent,
        readiness="DEPRECATED",
    )
    feat_namespace = await feature_namespace_service.update_document(
        document_id=feature.feature_namespace_id,
        data=FeatureNamespaceServiceUpdate(feature_ids=namespace.feature_ids + [new_feature_id]),
    )
    assert len(feat_namespace.feature_ids) == 2
    assert new_feature_id in feat_namespace.feature_ids
    assert feat_namespace.default_feature_id == feature.id
    assert feat_namespace.default_version_mode == "AUTO"
    assert feat_namespace.readiness == "DRAFT"

    # create another feature list version
    new_flist = await feature_list_service.create_document(
        data=FeatureListCreate(
            feature_ids=[new_feature_id],
            feature_list_namespace_id=feature_list.feature_list_namespace_id,
            name="sf_feature_list",
            version={"name": "V220914"},
        )
    )
    flist_namespace = await feature_list_namespace_service.get_document(
        document_id=feature_list.feature_list_namespace_id
    )
    assert len(flist_namespace.feature_list_ids) == 2
    assert new_flist.id in flist_namespace.feature_list_ids
    assert new_flist.feature_list_namespace_id == feature_list.feature_list_namespace_id
    assert flist_namespace.default_version_mode == "AUTO"
    assert flist_namespace.default_feature_list_id == feature_list.id
    assert flist_namespace.readiness_distribution.__root__ == [{"readiness": "DRAFT", "count": 1}]
    yield new_feature_id, new_flist.id
