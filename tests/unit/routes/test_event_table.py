"""
Tests for event tables route
"""
import datetime
import json
from http import HTTPStatus

import pytest

from featurebyte.models.event_table import EventTableModel, EventTableStatus
from tests.unit.models.test_event_table import (  # pylint: disable=unused-import
    event_table_model_dict_fixture,
)


@pytest.fixture(name="event_table_dict")
def event_table_dict_fixture(event_table_model_dict):
    """
    Table Event dict object
    """
    event_table_dict = json.loads(EventTableModel(**event_table_model_dict).json())
    event_table_dict.pop("created_at")
    return event_table_dict


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
    utcnow = datetime.datetime.utcnow()
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED
    result = response.json()
    assert datetime.datetime.fromisoformat(result.pop("created_at")) > utcnow
    result.pop("_id")
    assert result.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    # history should contain the initial entry
    event_table_dict.pop("history")
    history = result.pop("history")
    assert len(history) == 1
    assert history[0]["setting"] == event_table_dict["default_feature_job_setting"]
    # status should be draft
    event_table_dict.pop("status")
    assert result.pop("status") == EventTableStatus.DRAFT
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
    data = results["data"][0]
    data.pop("_id")
    data.pop("created_at")
    # should include the static user id
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert data.pop("history")[0]["setting"] == event_table_dict["default_feature_job_setting"]
    event_table_dict.pop("history")
    event_table_dict["status"] = EventTableStatus.DRAFT
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
    data.pop("created_at")
    # should include the static user id
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert data.pop("history")[0]["setting"] == event_table_dict["default_feature_job_setting"]
    event_table_dict.pop("history")
    event_table_dict["status"] = EventTableStatus.DRAFT
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
    data = response.json()
    previous_history = data["history"]

    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json=event_table_update_dict
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    data.pop("created_at")
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_table_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_table_dict.pop("default_feature_job_setting")
    new_history = data.pop("history")
    event_table_dict.pop("history")
    event_table_dict["status"] = EventTableStatus.DRAFT
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
    data = response.json()
    previous_history = data["history"]

    # expect status to be draft
    assert data["status"] == EventTableStatus.DRAFT

    event_table_update_dict["name"] = "Some other name"
    event_table_update_dict["source"] = "Some other source"
    event_table_update_dict["status"] = EventTableStatus.PUBLISHED
    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json=event_table_update_dict
    )
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    data.pop("created_at")
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_table_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_table_dict.pop("default_feature_job_setting")
    event_table_dict["history"] = []
    new_history = data.pop("history")
    event_table_dict.pop("history")
    assert data == event_table_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_table_update_dict["default_feature_job_setting"]

    # expect status to be updated to published
    assert data["status"] == EventTableStatus.PUBLISHED


def test_update_fails_invalid_transition(
    test_api_client, event_table_dict, event_table_update_dict
):
    """
    Update event table fails if status transition is no valid
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()

    # expect status to be draft
    assert data["status"] == EventTableStatus.DRAFT

    event_table_update_dict["status"] = EventTableStatus.DRAFT
    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json=event_table_update_dict
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {"detail": "Invalid status transition from DRAFT to DRAFT."}


def test_update_status_only(test_api_client, event_table_dict):
    """
    Update event table status only
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_table", json=event_table_dict)
    assert response.status_code == HTTPStatus.CREATED
    current_data = response.json()

    # expect status to be draft
    assert current_data.pop("status") == EventTableStatus.DRAFT

    response = test_api_client.request(
        "PATCH", url="/event_table/my_event_table", json={"status": EventTableStatus.PUBLISHED}
    )
    assert response.status_code == HTTPStatus.OK
    updated_data = response.json()

    # expect status to be published
    assert updated_data.pop("status") == EventTableStatus.PUBLISHED

    # the other fields should be unchanged
    assert updated_data == current_data
