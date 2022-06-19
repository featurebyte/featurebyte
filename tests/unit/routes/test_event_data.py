"""
Tests for Event Datas route
"""
import datetime
import json
from http import HTTPStatus
from unittest import mock

import pytest

from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.persistent import DuplicateDocumentError
from tests.unit.models.test_event_data import (  # pylint: disable=unused-import
    event_data_model_dict_fixture,
)


@pytest.fixture(name="event_data_dict")
def event_data_dict_fixture(event_data_model_dict):
    """
    Table Event dict object
    """
    event_data_dict = json.loads(EventDataModel(**event_data_model_dict).json())
    event_data_dict["name"] = "订单表"
    event_data_dict.pop("created_at")
    return event_data_dict


@pytest.fixture(name="event_data_update_dict")
def event_data_update_dict_fixture():
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


def test_create_success(test_api_client, event_data_dict):
    """
    Create Event Data
    """
    utcnow = datetime.datetime.utcnow()
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED
    result = response.json()
    assert datetime.datetime.fromisoformat(result.pop("created_at")) > utcnow
    result.pop("_id")
    assert result.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    # history should contain the initial entry
    event_data_dict.pop("history")
    history = result.pop("history")
    assert len(history) == 1
    assert history[0]["setting"] == event_data_dict["default_feature_job_setting"]
    # status should be draft
    event_data_dict.pop("status")
    assert result.pop("status") == EventDataStatus.DRAFT
    assert result == event_data_dict


def test_create_fails_table_exists(test_api_client, event_data_dict):
    """
    Create Event Data fails if table with same name already exists
    """
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {"detail": 'Event Data "订单表" already exists.'}


def test_create_fails_table_exists_during_insert(test_api_client, event_data_dict):
    """
    Create Event Data fails if table with same name already exists during persistent insert
    """
    with mock.patch("featurebyte.app.persistent.insert_one") as mock_insert:
        mock_insert.side_effect = DuplicateDocumentError
        response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {"detail": 'Event Data "订单表" already exists.'}


def test_create_fails_wrong_field_type(test_api_client, event_data_dict):
    """
    Create Event Data fails if wrong types are provided for fields
    """
    event_data_dict["source"] = "Some other source"
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
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


def test_list(test_api_client, event_data_dict):
    """
    List Event Datas
    """
    # insert a few records
    for i in range(3):
        event_data_dict["name"] = f"Table {i}"
        response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
        assert response.status_code == HTTPStatus.CREATED
    response = test_api_client.request("GET", url="/event_data")
    assert response.status_code == HTTPStatus.OK
    results = response.json()
    assert results["page"] == 1
    assert results["total"] == 3
    data = results["data"][0]
    data.pop("_id")
    data.pop("created_at")
    # should include the static user id
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert data.pop("history")[0]["setting"] == event_data_dict["default_feature_job_setting"]
    event_data_dict.pop("history")
    event_data_dict["status"] = EventDataStatus.DRAFT
    assert data == event_data_dict


def test_retrieve_success(test_api_client, event_data_dict):
    """
    Retrieve Event Data
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED

    # retrieve by table name
    response = test_api_client.request("GET", url="/event_data/订单表")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    data.pop("created_at")
    # should include the static user id
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"
    assert data.pop("history")[0]["setting"] == event_data_dict["default_feature_job_setting"]
    event_data_dict.pop("history")
    event_data_dict["status"] = EventDataStatus.DRAFT
    assert data == event_data_dict


def test_retrieve_fails_table_not_found(test_api_client):
    """
    Retrieve Event Data fails as it does not exist
    """
    # retrieve by table name
    response = test_api_client.request("GET", url="/event_data/订单表")
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {"detail": 'Event Data "订单表" not found.'}


def test_update_success(test_api_client, event_data_dict, event_data_update_dict):
    """
    Update Event Data
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    previous_history = data["history"]

    response = test_api_client.request("PATCH", url="/event_data/订单表", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    data.pop("created_at")
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_data_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_data_dict.pop("default_feature_job_setting")
    new_history = data.pop("history")
    event_data_dict.pop("history")
    event_data_dict["status"] = EventDataStatus.DRAFT
    assert data == event_data_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_data_update_dict["default_feature_job_setting"]


def test_update_fails_table_not_found(test_api_client, event_data_update_dict):
    """
    Update Event Data fails if table not found
    """
    response = test_api_client.request("PATCH", url="/event_data/订单表", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {"detail": 'Event Data "订单表" not found.'}


def test_update_excludes_unsupported_fields(
    test_api_client, event_data_dict, event_data_update_dict
):
    """
    Update Event Data only updates job settings even if other fields are provided
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()
    previous_history = data["history"]

    # expect status to be draft
    assert data["status"] == EventDataStatus.DRAFT

    event_data_update_dict["name"] = "Some other name"
    event_data_update_dict["source"] = "Some other source"
    event_data_update_dict["status"] = EventDataStatus.PUBLISHED
    response = test_api_client.request("PATCH", url="/event_data/订单表", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("_id")
    data.pop("created_at")
    assert data.pop("user_id") == "62a6d9d023e7a8f2a0dc041a"

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_data_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_data_dict.pop("default_feature_job_setting")
    event_data_dict["history"] = []
    new_history = data.pop("history")
    event_data_dict.pop("history")
    assert data == event_data_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_data_update_dict["default_feature_job_setting"]

    # expect status to be updated to published
    assert data["status"] == EventDataStatus.PUBLISHED


def test_update_fails_invalid_transition(test_api_client, event_data_dict, event_data_update_dict):
    """
    Update Event Data fails if status transition is no valid
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED
    data = response.json()

    # expect status to be draft
    assert data["status"] == EventDataStatus.DRAFT

    event_data_update_dict["status"] = EventDataStatus.DRAFT
    response = test_api_client.request("PATCH", url="/event_data/订单表", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {"detail": "Invalid status transition from DRAFT to DRAFT."}


def test_update_status_only(test_api_client, event_data_dict):
    """
    Update Event Data status only
    """
    # insert a record
    response = test_api_client.request("POST", url="/event_data", json=event_data_dict)
    assert response.status_code == HTTPStatus.CREATED
    current_data = response.json()

    # expect status to be draft
    assert current_data.pop("status") == EventDataStatus.DRAFT

    response = test_api_client.request(
        "PATCH", url="/event_data/订单表", json={"status": EventDataStatus.PUBLISHED}
    )
    assert response.status_code == HTTPStatus.OK
    updated_data = response.json()

    # expect status to be published
    assert updated_data.pop("status") == EventDataStatus.PUBLISHED

    # the other fields should be unchanged
    assert updated_data == current_data
