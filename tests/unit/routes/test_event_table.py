"""
Tests for event tables route
"""
import json
from http import HTTPStatus

import pytest

from featurebyte.models.event_table import EventTableModel
from tests.unit.models.test_event_table import (  # pylint: disable=unused-import
    event_table_model_dict_fixture,
)


@pytest.fixture(name="event_table_dict")
def event_table_dict_fixture(event_table_model_dict):
    """
    Table Event dict object
    """
    return json.loads(EventTableModel(**event_table_model_dict).json())


@pytest.fixture(name="event_table_update_dict")
def event_table_update_dict_fixture():
    """
    Table Event update dict object
    """
    return {
        "default_feature_job_setting": {
            "blind_spot": "12m",
            "frequency": "30m",
            "time_modulo_frequency": "5m",
        }
    }


def test_create_success(test_api_client, event_table_dict):
    """
    Create event table
    """
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED
    result = response.json()
    result.pop("_id")
    assert result.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert result == event_table_dict


def test_create_fails_table_exists(test_api_client, event_table_dict):
    """
    Create event table fails if table with same name already exists
    """
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {"detail": 'Event table "my_event_table" already exists.'}


def test_create_fails_wrong_field_type(test_api_client, event_table_dict):
    """
    Create event table fails if wrong types are provided for fields
    """
    event_table_dict["source"] = "Some other source"
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [
            {
                "loc": ["body", "source"],
                "msg": "value is not a valid dict",
                "type": "type_error.dict",
            }
        ]
    }


def test_list(test_api_client, event_table_dict):
    """
    List event tables
    """
    # insert a few records
    for i in range(3):
        event_table_dict["name"] = f"Table {i}"
        response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
        assert response.status_code == HTTPStatus.CREATED
    response = test_api_client.request("GET", url="/event_table")
    assert response.status_code == HTTPStatus.OK
    results = response.json()
    assert results["page"] == 1
    assert results["total"] == 3
    data = results["data"][-1]
    data.pop("_id")
    # should include the static user id
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert data == event_table_dict


def test_retrieve_success(test_api_client, event_table_dict):
    """
    Retrieve event table
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED

    # retrieve by table name
    response = test_api_client.request("GET", url="/event_table/my_event_table")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    # should include the static user id
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert data == event_table_dict


def test_retrieve_fails_table_not_found(test_api_client):
    """
    Retrieve event table fails as it does not exist
    """
    # retrieve by table name
    response = test_api_client.request("GET", url="/event_table/my_event_table")
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {"detail": 'Event table "my_event_table" not found.'}


def test_update_success(test_api_client, event_table_dict, event_table_update_dict):
    """
    Update event table
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED

    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json=event_table_update_dict
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_table_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_table_dict.pop("default_feature_job_setting")
    new_history = data.pop("history")
    previous_history = event_table_dict.pop("history")
    assert data == event_table_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_table_update_dict["default_feature_job_setting"]


def test_update_fails_table_not_found(test_api_client, event_table_update_dict):
    """
    Update event table fails if table not found
    """
    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json=event_table_update_dict
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {"detail": 'Event table "my_event_table" not found.'}


def test_update_excludes_unsupported_fields(
    test_api_client, event_table_dict, event_table_update_dict
):
    """
    Update event table only updates job settings even if other fields are provided
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED

    event_table_update_dict["name"] = "Some other name"
    event_table_update_dict["source"] = "Some other source"
    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json=event_table_update_dict
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_table_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_table_dict.pop("default_feature_job_setting")
    new_history = data.pop("history")
    previous_history = event_table_dict.pop("history")
    assert data == event_table_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_table_update_dict["default_feature_job_setting"]
