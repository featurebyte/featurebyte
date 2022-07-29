"""
Tests for Feature route
"""
from datetime import datetime
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.api.event_data import EventData
from featurebyte.exception import DuplicatedFeatureRegistryError
from featurebyte.models.feature import FeatureModel, FeatureReadiness
from featurebyte.models.feature_store import (
    FeatureStoreModel,
    SourceType,
    SQLiteDetails,
    TableDetails,
)
from featurebyte.persistent.mongo import MongoDB
from featurebyte.routes.feature.controller import FeatureController


@pytest.fixture(name="feature_model_dict")
def feature_model_dict_fixture(feature_model_dict):
    """
    Feature model dict fixture
    """
    feature_model_dict["_id"] = str(ObjectId())
    return feature_model_dict


@pytest.fixture(name="snowflake_event_data")
def snowflake_event_data_fixture(snowflake_database_table, config):
    """
    EventData object fixture
    """
    yield EventData.from_tabular_source(
        tabular_source=snowflake_database_table,
        name="sf_event_data",
        event_timestamp_column="event_timestamp",
        record_creation_date_column="created_at",
        credentials=config.credentials,
    )


@pytest.fixture(name="mock_insert_feature_registry")
def mock_insert_feature_registry_fixture():
    """
    Mock insert feature registry at the controller level
    """
    with patch(
        "featurebyte.routes.feature.controller.FeatureController.insert_feature_registry"
    ) as mock:
        yield mock


@pytest.fixture(name="create_success_response")
def create_success_response_fixture(
    test_api_client_persistent,
    feature_model_dict,
    snowflake_event_data,
    mock_insert_feature_registry,
):
    """
    Post create success response fixture
    """
    _ = mock_insert_feature_registry
    test_api_client, persistent = test_api_client_persistent
    if isinstance(persistent, MongoDB):
        pytest.skip("Session not supported in Mongomock!")

    snowflake_event_data.save()
    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    response = test_api_client.post("/feature", json=feature_model_dict)
    return response


@pytest.mark.asyncio
async def test_create_201(
    test_api_client_persistent,
    feature_model_dict,
    create_success_response,
    mock_insert_feature_registry,
):
    """
    Test feature creation
    """
    _ = mock_insert_feature_registry
    test_api_client, persistent = test_api_client_persistent

    # check response
    assert create_success_response.status_code == HTTPStatus.CREATED
    result = create_success_response.json()
    feature_id = result.pop("_id")
    feature_readiness = result.pop("readiness")
    created_at = datetime.fromisoformat(result.pop("created_at"))
    updated_at = result.pop("updated_at")
    assert result.pop("user_id") is None
    assert created_at < datetime.utcnow()
    assert updated_at is None
    assert feature_readiness == "DRAFT"
    for key in ["tabular_source", "lineage", "row_index_lineage"]:
        assert tuple(result.pop(key)) == feature_model_dict[key]
    for key in result.keys():
        assert result[key] == feature_model_dict[key]

    # check feature namespace
    feat_namespace_docs, match_count = await persistent.find(
        collection_name="feature_namespace",
        query_filter={"name": feature_model_dict["name"]},
    )
    assert match_count == 1
    assert feat_namespace_docs[0]["name"] == feature_model_dict["name"]
    assert feat_namespace_docs[0]["description"] is None
    assert feat_namespace_docs[0]["versions"] == ["V220710"]
    assert feat_namespace_docs[0]["version_ids"] == [ObjectId(feature_id)]
    assert feat_namespace_docs[0]["readiness"] == feature_readiness
    assert feat_namespace_docs[0]["default_version_id"] == ObjectId(feature_id)
    assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
    assert feat_namespace_docs[0]["created_at"] > created_at
    assert feat_namespace_docs[0]["updated_at"] == updated_at

    # create a new feature version with the same namespace
    feature_model_dict["_id"] = str(ObjectId())
    feature_model_dict["parent_id"] = feature_id
    feature_model_dict["event_data_ids"] = result["event_data_ids"]
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.CREATED
    new_version_result = response.json()

    # check feature namespace
    feat_namespace_docs, match_count = await persistent.find(
        collection_name="feature_namespace",
        query_filter={"name": feature_model_dict["name"]},
    )
    assert match_count == 1
    assert feat_namespace_docs[0]["name"] == feature_model_dict["name"]
    assert feat_namespace_docs[0]["description"] is None
    assert feat_namespace_docs[0]["version_ids"] == [
        ObjectId(feature_id),
        ObjectId(new_version_result["_id"]),
    ]
    assert feat_namespace_docs[0]["versions"] == ["V220710", "V220710_1"]
    assert feat_namespace_docs[0]["readiness"] == feature_readiness
    assert feat_namespace_docs[0]["default_version_id"] == ObjectId(new_version_result["_id"])
    assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
    assert feat_namespace_docs[0]["created_at"] > created_at
    assert feat_namespace_docs[0]["updated_at"] > feat_namespace_docs[0]["created_at"]


def test_create_409(
    create_success_response, test_api_client_persistent, feature_model_dict, snowflake_event_data
):
    """
    Test feature creation (conflict)
    """
    test_api_client, _ = test_api_client_persistent

    # simulate the case when the feature id has been saved
    create_result = create_success_response.json()
    feature_model_dict["_id"] = create_result["_id"]
    response = test_api_client.post("/feature", json=feature_model_dict)
    feature_id = feature_model_dict.pop("_id")
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": f'Feature (feature.id: "{feature_id}") has been saved before.'
    }

    feature_model_dict["_id"] = str(ObjectId())
    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    assert feature_model_dict["parent_id"] is None
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": 'Feature name (feature.name: "sum_30m") already exists.'}


def test_create_422__not_proper_parent(
    create_success_response, test_api_client_persistent, feature_model_dict, snowflake_event_data
):
    """
    Test feature creation (when the parent id)
    """
    test_api_client, _ = test_api_client_persistent
    feature_id = str(ObjectId())
    parent_id = str(ObjectId())
    feature_model_dict["_id"] = feature_id
    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    feature_model_dict["parent_id"] = parent_id
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": (
            f'The original feature (feature.id: "{parent_id}") not found! '
            f"Please save the Feature object first."
        )
    }

    create_result = create_success_response.json()
    parent_id = create_result["_id"]
    feature_model_dict["name"] = "other_name"
    feature_model_dict["parent_id"] = parent_id
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": (
            f'Feature (feature.id: "{feature_id}", feature.parent_id: "{parent_id}") '
            f"has invalid parent feature!"
        )
    }


def test_create_422(test_api_client_persistent, feature_model_dict):
    """
    Test feature creation (unprocessable entity due to missing event data ids)
    """
    test_api_client, persistent = test_api_client_persistent
    if isinstance(persistent, MongoDB):
        pytest.skip("Session not supported in Mongomock!")

    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json()["detail"] == [
        {
            "loc": ["body", "event_data_ids"],
            "msg": "ensure this value has at least 1 items",
            "type": "value_error.list.min_items",
            "ctx": {"limit_value": 1},
        }
    ]

    # test unsaved event ids
    unknown_event_id = str(ObjectId())
    feature_model_dict["event_data_ids"] = [unknown_event_id]
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json()["detail"] == (
        f'EventData (event_data.id: "{unknown_event_id}") not found! '
        f"Please save the EventData object first."
    )


@pytest.mark.parametrize(
    "version_ids,versions,readiness,default_version_mode,default_version,doc_id,doc_version,expected",
    [
        [
            # for auto mode, update default version to the best readiness of the most recent version
            # the best readiness before update is DRAFT, new version will replace the default version
            ["feature_id1"],
            ["V220710"],
            FeatureReadiness.DRAFT,
            "AUTO",
            "feature_id1",
            "feature_id2",
            "V220711",
            {
                "versions": ["V220710", "V220711"],
                "version_ids": ["feature_id1", "feature_id2"],
                "readiness": "DRAFT",
                "default_version_id": "feature_id2",
            },
        ],
        [
            # for manual mode, won't update default feature version
            ["feature_id1"],
            ["V220710"],
            FeatureReadiness.DRAFT,
            "MANUAL",
            "feature_id1",
            "feature_id2",
            "V220710",
            {
                "versions": ["V220710", "V220710_1"],
                "version_ids": ["feature_id1", "feature_id2"],
                "readiness": "DRAFT",
                "default_version_id": "feature_id1",
            },
        ],
        [
            # for auto mode, update default version to the best readiness of the most recent version
            # the best readiness before update is PRODUCTION_READY, new version will not replace the default version
            ["feature_id1"],
            ["V220710"],
            FeatureReadiness.PRODUCTION_READY,
            "AUTO",
            "feature_id1",
            "feature_id2",
            "V220710",
            {
                "versions": ["V220710", "V220710_1"],
                "version_ids": ["feature_id1", "feature_id2"],
                "readiness": "PRODUCTION_READY",
                "default_version_id": "feature_id1",
            },
        ],
    ],
)
def test_prepare_feature_namespace_payload(
    version_ids,
    versions,
    readiness,
    default_version_mode,
    default_version,
    doc_id,
    doc_version,
    expected,
):
    """
    Test prepare_feature_namespace_payload function
    """
    feature_namespace = Mock()
    feature_namespace.version_ids = version_ids
    feature_namespace.versions = versions
    feature_namespace.readiness = readiness
    feature_namespace.default_version_mode = default_version_mode
    feature_namespace.default_version_id = default_version
    feature = Mock()
    feature.id = doc_id
    feature.version = doc_version
    assert (
        FeatureController.prepare_feature_namespace_payload(
            document=feature, feature_namespace=feature_namespace
        )
        == expected
    )


@pytest.fixture(name="get_credential")
def get_credential_fixture(config):
    """
    get_credential fixture
    """

    def get_credential(user_id, feature_store):
        _ = user_id
        return config.credentials.get(feature_store)

    return get_credential


@patch("featurebyte.session.base.BaseSession.execute_query")
def test_insert_feature_register(
    mock_execute_query, feature_model_dict, snowflake_connector, get_credential
):
    """
    Test insert_feature_registry
    """
    _ = snowflake_connector
    user = Mock()
    feature = FeatureModel(**feature_model_dict)
    FeatureController.insert_feature_registry(
        user=user, document=feature, get_credential=get_credential
    )

    match_count = 0
    expected_partial_query = "INSERT INTO FEATURE_REGISTRY"
    for call_args in mock_execute_query.call_args_list:
        if expected_partial_query in call_args.args[0]:
            match_count += 1
    assert match_count > 0


@patch("featurebyte.session.base.BaseSession.execute_query")
def test_insert_feature_registry__non_snowflake_feature_store(
    mock_execute_query, feature_model_dict
):
    """
    Test insert_feature_registry function (when feature store is not snowflake)
    """
    feature_store = FeatureStoreModel(
        type=SourceType.SQLITE, details=SQLiteDetails(filename="some_filename")
    )
    feature_model_dict["tabular_source"] = (feature_store, TableDetails(table_name="some_table"))
    feature = FeatureModel(**feature_model_dict)
    user, get_credential = Mock(), Mock()
    FeatureController.insert_feature_registry(
        user=user, document=feature, get_credential=get_credential
    )
    assert mock_execute_query.call_count == 0


@patch("featurebyte.routes.feature.controller.FeatureManagerSnowflake")
def test_insert_feature_registry__duplicated_feature_registry_exception(
    mock_feature_manager, feature_model_dict, get_credential
):
    """
    Test insert_feature_registry with duplicated_feature_registry exception
    """
    mock_feature_manager.return_value.insert_feature_registry.side_effect = (
        DuplicatedFeatureRegistryError
    )
    feature = FeatureModel(**feature_model_dict)
    user = Mock()
    with pytest.raises(HTTPException) as exc:
        FeatureController.insert_feature_registry(
            user=user, document=feature, get_credential=get_credential
        )
    expected_msg = (
        'Feature (feature.name: "sum_30m") has been registered by other feature '
        "at Snowflake feature store."
    )
    assert expected_msg in str(exc.value.detail)
    assert not mock_feature_manager.return_value.remove_feature_registry.called


@patch("featurebyte.routes.feature.controller.FeatureManagerSnowflake")
def test_insert_feature_registry__other_exception(
    mock_feature_manager, feature_model_dict, get_credential
):
    """
    Test insert_feature_registry with non duplicated feature registry exception
    """
    mock_feature_manager.return_value.insert_feature_registry.side_effect = ValueError
    feature = FeatureModel(**feature_model_dict)
    user = Mock()
    with pytest.raises(ValueError):
        FeatureController.insert_feature_registry(
            user=user, document=feature, get_credential=get_credential
        )
    assert mock_feature_manager.return_value.remove_feature_registry.called
