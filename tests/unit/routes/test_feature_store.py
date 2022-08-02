"""
Test for FeatureStore route
"""
import pdb
from datetime import datetime
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import SourceType


@pytest.fixture(name="feature_store_dict")
def feature_store_dict_fixture():
    """
    FeatureStore model dictionary fixture
    """
    return {
        "_id": str(ObjectId()),
        "name": "my_feature_store",
        "type": SourceType.SNOWFLAKE,
        "details": {
            "account": "sf_account",
            "warehouse": "sf_warehouse",
            "database": "sf_database",
            "sf_schema": "sf_schema",
        },
    }


@pytest.fixture(name="create_success_response")
def create_success_response_fixture(
    test_api_client_persistent,
    feature_store_dict,
):
    test_api_client, persistent = test_api_client_persistent
    response = test_api_client.post("/feature_store", json=feature_store_dict)
    return response


def test_create_201(create_success_response):
    """
    Test feature store creation (success)
    """
    utcnow = datetime.utcnow()
    assert create_success_response.status_code == HTTPStatus.CREATED
    result = create_success_response.json()

    # check response
    _ = ObjectId(result["_id"])  # valid ObjectId
    assert result["user_id"] is None
    assert datetime.fromisoformat(result["created_at"]) < utcnow
    assert result["updated_at"] is None


def test_create_409(create_success_response, test_api_client_persistent, feature_store_dict):
    """
    Test feature store creation (conflict)
    """
    test_api_client, _ = test_api_client_persistent
    _ = create_success_response
    response = test_api_client.post("/feature_store", json=feature_store_dict)
    feature_store_id = feature_store_dict["_id"]
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": f'FeatureStore id (feature_store.id: "{feature_store_id}") already exists.'
    }

    feature_store_dict["_id"] = str(ObjectId())
    response = test_api_client.post("/feature_store", json=feature_store_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {
        "detail": 'FeatureStore name (feature_store.name: "my_feature_store") already exists.'
    }


def test_create_422(test_api_client_persistent, feature_store_dict):
    """
    Test feature store creation (unprocessable entity)
    """
    test_api_client, _ = test_api_client_persistent
    feature_store_dict.pop("_id")
    response = test_api_client.post("/feature_store", json=feature_store_dict)
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "_id"],
                "msg": "field required",
                "type": "value_error.missing",
            }
        ]
    }


def test_list_200(create_success_response, test_api_client_persistent, feature_store_dict):
    """
    Test list entities (success)
    """
    test_api_client, _ = test_api_client_persistent
    _ = create_success_response
    response = test_api_client.get("/feature_store")
    assert response.status_code == HTTPStatus.OK
    result = response.json()
    expected_paginated_info = {"page": 1, "page_size": 10, "total": 1}

    result_data = result.pop("data")
    assert len(result_data) == 1
    assert result_data[0].items() >= feature_store_dict.items()
    assert result == expected_paginated_info


def test_get_200(create_success_response, test_api_client_persistent):
    """
    Test get feature store (success)
    """
    test_api_client, _ = test_api_client_persistent
    created_feature_store = create_success_response.json()
    feature_store_id = created_feature_store["_id"]
    response = test_api_client.get(f"/feature_store/{feature_store_id}")
    assert response.status_code == HTTPStatus.OK
    response_data = response.json()
    assert response_data == created_feature_store


def test_get_404(test_api_client_persistent):
    """
    Test get feature store (not found)
    """
    test_api_client, _ = test_api_client_persistent
    unknown_feature_store_id = ObjectId()
    response = test_api_client.get(f"/feature_store/{unknown_feature_store_id}")
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": (
            f'FeatureStore (feature_store.id: "{unknown_feature_store_id}") not found! '
            "Please save the FeatureStore object first."
        )
    }
