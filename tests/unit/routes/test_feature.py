"""
Tests for Feature route
"""
from datetime import datetime
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.api.event_data import EventData
from featurebyte.persistent.mongo import MongoDB


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


@pytest.fixture(name="feature_dict")
def feature_dict_fixture(feature_model_dict, snowflake_event_data):
    """
    Feature dictionary fixture
    """
    snowflake_event_data.save_as_draft()
    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    return feature_model_dict


@pytest.fixture(name="create_success_response")
def create_success_response_fixture(
    test_api_client_persistent, feature_model_dict, snowflake_event_data
):
    """
    Post create success response fixture
    """
    test_api_client, persistent = test_api_client_persistent
    if isinstance(persistent, MongoDB):
        pytest.skip("Session not supported in Mongomock!")

    snowflake_event_data.save_as_draft()
    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    response = test_api_client.post("/feature", json=feature_model_dict)
    return response


def test_create_201(test_api_client_persistent, feature_model_dict, create_success_response):
    """
    Test feature creation
    """
    test_api_client, persistent = test_api_client_persistent

    # check response
    assert create_success_response.status_code == HTTPStatus.CREATED
    result = create_success_response.json()
    feature_id = result.pop("id")
    feature_readiness = result.pop("readiness")
    created_at = datetime.fromisoformat(result.pop("created_at"))
    assert result.pop("user_id") is None
    assert created_at < datetime.utcnow()
    assert feature_readiness == "DRAFT"
    for key in ["tabular_source", "lineage", "row_index_lineage"]:
        assert tuple(result.pop(key)) == feature_model_dict[key]
    for key in result.keys():
        assert result[key] == feature_model_dict[key]

    # check feature namespace
    feat_namespace_docs, match_count = persistent.find(
        collection_name="feature_namespace",
        query_filter={"name": feature_model_dict["name"]},
    )
    assert match_count == 1
    assert feat_namespace_docs[0]["name"] == feature_model_dict["name"]
    assert feat_namespace_docs[0]["description"] is None
    assert feat_namespace_docs[0]["versions"] == [ObjectId(feature_id)]
    assert feat_namespace_docs[0]["readiness"] == feature_readiness
    assert feat_namespace_docs[0]["default_version_id"] == ObjectId(feature_id)
    assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
    assert feat_namespace_docs[0]["created_at"] == created_at

    # create a new feature version with the same namespace
    feature_model_dict["parent_id"] = feature_id
    feature_model_dict["event_data_ids"] = result["event_data_ids"]
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.CREATED
    new_version_result = response.json()

    # check feature namespace
    feat_namespace_docs, match_count = persistent.find(
        collection_name="feature_namespace",
        query_filter={"name": feature_model_dict["name"]},
    )
    assert match_count == 1
    assert feat_namespace_docs[0]["name"] == feature_model_dict["name"]
    assert feat_namespace_docs[0]["description"] is None
    assert feat_namespace_docs[0]["versions"] == [
        ObjectId(feature_id),
        ObjectId(new_version_result["id"]),
    ]
    assert feat_namespace_docs[0]["readiness"] == feature_readiness
    assert feat_namespace_docs[0]["default_version_id"] == ObjectId(new_version_result["id"])
    assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
    assert feat_namespace_docs[0]["created_at"] == created_at


def test_create_409(
    create_success_response, test_api_client_persistent, feature_model_dict, snowflake_event_data
):
    """
    Test feature creation (conflict)
    """
    test_api_client, _ = test_api_client_persistent

    # simulate the case when the feature id has been saved
    create_result = create_success_response.json()
    feature_model_dict["id"] = create_result["id"]
    response = test_api_client.post("/feature", json=feature_model_dict)
    feature_id = feature_model_dict.pop("id")
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": f'Feature ID "{feature_id}" already exists.'}

    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    assert feature_model_dict["parent_id"] is None
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": 'Feature name "sum_30m" already exists.'}


def test_create_422__not_proper_parent(
    create_success_response, test_api_client_persistent, feature_model_dict, snowflake_event_data
):
    """
    Test feature creation (when the parent id)
    """
    test_api_client, _ = test_api_client_persistent
    parent_id = str(ObjectId())
    feature_model_dict["event_data_ids"] = [str(snowflake_event_data.id)]
    feature_model_dict["parent_id"] = parent_id
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": f'Feature ID "{parent_id}" not found! Please save the parent Feature object.'
    }

    create_result = create_success_response.json()
    feature_model_dict["name"] = "other_name"
    feature_model_dict["parent_id"] = create_result["id"]
    parent_id = create_result["id"]
    response = test_api_client.post("/feature", json=feature_model_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": f'Feature ID "{parent_id}" is not a valid parent feature!'}


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
        f'EventData ID "{unknown_event_id}" not found! Please save the EventData object.'
    )
