"""
Test for FeatureStore route
"""
from datetime import datetime
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import SourceType


@pytest.fixture(name="feature_store_model_dict")
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
    feature_store_model_dict,
):
    test_api_client, persistent = test_api_client_persistent
    response = test_api_client.post("/feature_store", json=feature_store_model_dict)
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
    assert datetime.fromisoformat(result["created_at"]) < utcnow
    assert result["updated_at"] is None
