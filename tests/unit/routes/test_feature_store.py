"""
Test for FeatureStore routes
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId


@pytest.fixture(name="feature_store_dict")
def feature_store_dict_fixture():
    """
    FeatureStore dictionary payload
    """
    return {
        "_id": str(ObjectId()),
        "name": "my_feature_store",
        "type": "snowflake",
        "details": {
            "account": "sf_account",
            "warehouse": "sf_warehouse",
            "database": "sf_database",
            "sf_schema": "sf_schema",
        },
    }


@pytest.fixture(name="feature_store_response")
def feature_store_response_fixture(test_api_client_persistent, feature_store_dict):
    """
    Feature store response fixture
    """
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.post("/feature_store", json=feature_store_dict)
    assert response.status_code == HTTPStatus.CREATED
    return response


def test_create_success(feature_store_response, feature_store_dict):
    """
    Create Feature Store record
    """
    response_dict = feature_store_response.json()
    assert response_dict["_id"] == feature_store_dict["_id"]
    # check that feature_store dictionary is a subset of response dictionary
    assert feature_store_dict.items() <= response_dict.items()


def test_create_conflict(feature_store_response, feature_store_dict, test_api_client_persistent):
    """
    Create Feature Store failure due to name/id conflict
    """
    test_api_client, _ = test_api_client_persistent
    name_conflict_payload = feature_store_dict.copy()
    name_conflict_payload["_id"] = str(ObjectId())
    response = test_api_client.post("/feature_store", json=name_conflict_payload)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": 'FeatureStore (feature_store.name: "my_feature_store") already exists.'
    }

    id_conflict_payload = feature_store_dict.copy()
    id_conflict_payload["name"] = "other_name"
    feature_store_id = feature_store_dict["_id"]
    response = test_api_client.post("/feature_store", json=id_conflict_payload)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": f'FeatureStore (feature_store.id: "{feature_store_id}") already exists.'
    }


def test_get_success(feature_store_response, feature_store_dict, test_api_client_persistent):
    """
    Retrieve Feature Store success
    """
    test_api_client, _ = test_api_client_persistent
    feature_store_id = feature_store_dict["_id"]
    response = test_api_client.get(f"/feature_store/{feature_store_id}")
    assert response.status_code == HTTPStatus.OK
    response_dict = response.json()
    # check that feature_store dictionary is a subset of response dictionary
    assert feature_store_dict.items() <= response_dict.items()


def test_get_failure__not_found(test_api_client_persistent):
    """
    Feature Store retrieval failure
    """
    test_api_client, _ = test_api_client_persistent
    random_id = ObjectId()
    response = test_api_client.get(f"/feature_store/{random_id}")
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": f'FeatureStore (feature_store.id: "{random_id}") not found!'
    }


def test_list_feature_store(test_api_client_persistent, feature_store_response):
    """
    Test listing Feature Stores
    """
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.get("/feature_store")
    assert response.status_code == HTTPStatus.OK
    response_dict = response.json()
    assert response_dict == {
        "page": 1,
        "page_size": 10,
        "total": 1,
        "data": [feature_store_response.json()],
    }
