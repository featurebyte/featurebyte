"""
Tests for Entity route
"""
from datetime import datetime
from http import HTTPStatus

import pytest
from bson import ObjectId


@pytest.fixture(name="entity_dict")
def entity_dict_fixture():
    """
    Entity dictionary fixture
    """
    return {"name": "customer", "serving_column_names": ["cust_id"]}


def test_create_success(test_api_client, entity_dict):
    """
    Test entity creation
    """
    utcnow = datetime.utcnow()
    response = test_api_client.post("/entity", json=entity_dict)
    assert response.status_code == HTTPStatus.CREATED
    result = response.json()

    # check response
    _ = ObjectId(result.pop("id"))  # valid ObjectId
    assert result.pop("user_id") is None
    assert datetime.fromisoformat(result.pop("created_at")) > utcnow
    assert result == entity_dict
