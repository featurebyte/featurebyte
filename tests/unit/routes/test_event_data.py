"""
Tests for Event Datas route
"""
import datetime
import json
from http import HTTPStatus
from unittest import mock

import pytest
from bson import ObjectId

from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.persistent import DuplicateDocumentError
from featurebyte.persistent.git import GitDB


@pytest.fixture(name="event_data_model_dict")
def event_data_model_dict_fixture(event_data_model_dict):
    """
    EventData dict object
    """
    event_data_model_dict = json.loads(EventDataModel(**event_data_model_dict).json(by_alias=True))
    event_data_model_dict["name"] = "订单表"
    event_data_model_dict.pop("created_at")
    event_data_model_dict.pop("updated_at")
    return event_data_model_dict


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


@pytest.fixture(name="event_data_response")
def event_data_response_fixture(test_api_client_persistent, event_data_model_dict):
    """
    Event data response fixture
    """
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.post("/event_data", json=event_data_model_dict)
    assert response.status_code == HTTPStatus.CREATED
    assert response.json()["_id"] == event_data_model_dict["_id"]
    yield response


def test_create_success(event_data_response, event_data_model_dict):
    """
    Create Event Data
    """
    utcnow = datetime.datetime.utcnow()
    assert event_data_response.status_code == HTTPStatus.CREATED
    result = event_data_response.json()
    assert datetime.datetime.fromisoformat(result.pop("created_at")) < utcnow
    assert result.pop("updated_at") is None
    # history should contain the initial entry
    event_data_model_dict.pop("history")
    history = result.pop("history")
    assert len(history) == 1
    assert history[0]["setting"] == event_data_model_dict["default_feature_job_setting"]
    # status should be draft
    event_data_model_dict.pop("status")
    assert result.pop("status") == EventDataStatus.DRAFT
    assert result == event_data_model_dict


def test_create_fails_table_exists(
    test_api_client_persistent, event_data_model_dict, event_data_response
):
    """
    Create Event Data fails if table with same name already exists
    """
    _ = event_data_response
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.post("/event_data", json=event_data_model_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": 'EventData (event_data.name: "订单表") already exists.'}


def test_create_fails_table_exists_during_insert(test_api_client_persistent, event_data_model_dict):
    """
    Create Event Data fails if table with same name already exists during persistent insert
    """
    test_api_client, persistent = test_api_client_persistent
    if isinstance(persistent, GitDB):
        func_path = "featurebyte.persistent.GitDB.insert_one"
    else:
        func_path = "featurebyte.persistent.MongoDB.insert_one"
    with mock.patch(func_path) as mock_insert:
        mock_insert.side_effect = DuplicateDocumentError
        response = test_api_client.post("/event_data", json=event_data_model_dict)
    assert response.status_code == HTTPStatus.CONFLICT
    assert response.json() == {"detail": 'EventData (event_data.name: "订单表") already exists.'}


def test_create_fails_wrong_field_type(test_api_client_persistent, event_data_model_dict):
    """
    Create Event Data fails if wrong types are provided for fields
    """
    test_api_client, _ = test_api_client_persistent
    event_data_model_dict["tabular_source"] = ("Some other source", "other_table")
    response = test_api_client.post("/event_data", json=event_data_model_dict)
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {
        "detail": [
            {
                "ctx": {"object_type": "FeatureStoreModel"},
                "loc": ["body", "tabular_source", 0],
                "msg": "value is not a valid FeatureStoreModel type",
                "type": "type_error.featurebytetype",
            },
            {
                "ctx": {"object_type": "TableDetails"},
                "loc": ["body", "tabular_source", 1],
                "msg": "value is not a valid TableDetails type",
                "type": "type_error.featurebytetype",
            },
        ]
    }


@pytest.fixture(name="inserted_event_data_ids")
def inserted_event_data_ids_fixture(test_api_client_persistent, event_data_model_dict):
    """
    Inserted multiple event data results & return its ids as fixture
    """
    test_api_client, _ = test_api_client_persistent
    # insert a few records
    insert_ids = []
    for i in range(3):
        insert_id = str(ObjectId())
        event_data_model_dict["_id"] = insert_id
        event_data_model_dict["name"] = f"Table {i}"
        response = test_api_client.post("/event_data", json=event_data_model_dict)
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["_id"] == insert_id
        insert_ids.append(insert_id)
    yield insert_ids


def test_list(inserted_event_data_ids, test_api_client_persistent, event_data_model_dict):
    """
    List Event Datas
    """
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.get("/event_data")
    assert response.status_code == HTTPStatus.OK
    results = response.json()
    assert results["page"] == 1
    assert results["total"] == 3
    data = results["data"][0]
    assert data["_id"] == inserted_event_data_ids[-1]
    data.pop("created_at")
    data.pop("updated_at")

    # should include the static user id
    assert data.pop("history")[0]["setting"] == event_data_model_dict["default_feature_job_setting"]
    event_data_model_dict.pop("history")
    event_data_model_dict["status"] = EventDataStatus.DRAFT
    assert data == event_data_model_dict


def test_list__not_supported(inserted_event_data_ids, test_api_client_persistent):
    """
    Test search event data by search term
    """
    _ = inserted_event_data_ids
    test_api_client, _ = test_api_client_persistent
    response = test_api_client.get("/event_data", params={"search": "abc"})
    assert response.status_code == HTTPStatus.NOT_IMPLEMENTED
    results = response.json()
    assert results == {"detail": "Query not supported."}


def test_retrieve_success(test_api_client_persistent, event_data_model_dict, event_data_response):
    """
    Retrieve Event Data
    """
    test_api_client, _ = test_api_client_persistent
    insert_id = event_data_response.json()["_id"]

    # retrieve by event data id
    response = test_api_client.get(f"/event_data/{insert_id}")
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    data.pop("created_at")
    data.pop("updated_at")
    # should include the static user id
    assert data.pop("history")[0]["setting"] == event_data_model_dict["default_feature_job_setting"]
    event_data_model_dict.pop("history")
    event_data_model_dict["status"] = EventDataStatus.DRAFT
    assert data == event_data_model_dict


def test_retrieve_fails_table_not_found(test_api_client_persistent):
    """
    Retrieve Event Data fails as it does not exist
    """
    # retrieve by table name
    test_api_client, _ = test_api_client_persistent
    random_id = ObjectId()
    response = test_api_client.get(f"/event_data/{random_id}")
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": f'EventData (event_data.id: "{random_id}") not found! Please save the EventData object first.'
    }


def test_update_success(
    test_api_client_persistent, event_data_response, event_data_update_dict, event_data_model_dict
):
    """
    Update Event Data
    """
    test_api_client, _ = test_api_client_persistent
    response_dict = event_data_response.json()
    insert_id = response_dict["_id"]
    previous_history = response_dict["history"]

    response = test_api_client.patch(f"/event_data/{insert_id}", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.OK
    update_response_dict = response.json()
    assert update_response_dict["_id"] == insert_id
    update_response_dict.pop("created_at")
    update_response_dict.pop("updated_at")

    # default_feature_job_setting should be updated
    assert (
        update_response_dict.pop("default_feature_job_setting")
        == event_data_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_data_model_dict.pop("default_feature_job_setting")
    new_history = update_response_dict.pop("history")
    event_data_model_dict.pop("history")
    event_data_model_dict["status"] = EventDataStatus.DRAFT
    assert update_response_dict == event_data_model_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_data_update_dict["default_feature_job_setting"]


def test_update_fails_table_not_found(test_api_client_persistent, event_data_update_dict):
    """
    Update Event Data fails if table not found
    """
    test_api_client, _ = test_api_client_persistent
    random_id = ObjectId()
    response = test_api_client.patch(f"/event_data/{random_id}", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        "detail": f'EventData (event_data.id: "{random_id}") not found! Please save the EventData object first.'
    }


def test_update_excludes_unsupported_fields(
    test_api_client_persistent, event_data_response, event_data_update_dict, event_data_model_dict
):
    """
    Update Event Data only updates job settings even if other fields are provided
    """
    test_api_client, _ = test_api_client_persistent
    response_dict = event_data_response.json()
    insert_id = response_dict["_id"]
    assert insert_id
    previous_history = response_dict["history"]

    # expect status to be draft
    assert response_dict["status"] == EventDataStatus.DRAFT

    event_data_update_dict["name"] = "Some other name"
    event_data_update_dict["source"] = "Some other source"
    event_data_update_dict["status"] = EventDataStatus.PUBLISHED
    response = test_api_client.patch(f"/event_data/{insert_id}", json=event_data_update_dict)
    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data["_id"] == insert_id
    data.pop("created_at")
    data.pop("updated_at")

    # default_feature_job_setting should be updated
    assert (
        data.pop("default_feature_job_setting")
        == event_data_update_dict["default_feature_job_setting"]
    )

    # the other fields should be unchanged
    event_data_model_dict.pop("default_feature_job_setting")
    event_data_model_dict["history"] = []
    new_history = data.pop("history")
    event_data_model_dict.pop("history")
    assert data == event_data_model_dict

    # history should be appended with new default job settings update
    assert len(new_history) == len(previous_history) + 1
    assert new_history[1:] == previous_history
    assert new_history[0]["setting"] == event_data_update_dict["default_feature_job_setting"]

    # expect status to be updated to published
    assert data["status"] == EventDataStatus.PUBLISHED


def test_update_fails_invalid_transition(
    test_api_client_persistent, event_data_response, event_data_update_dict
):
    """
    Update Event Data fails if status transition is no valid
    """
    test_api_client, _ = test_api_client_persistent
    response_dict = event_data_response.json()
    event_data_update_dict["status"] = EventDataStatus.DRAFT
    response = test_api_client.patch(
        f"/event_data/{response_dict['_id']}", json=event_data_update_dict
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
    assert response.json() == {"detail": "Invalid status transition from DRAFT to DRAFT."}


def test_update_status_only(test_api_client_persistent, event_data_response):
    """
    Update Event Data status only
    """
    # insert a record
    test_api_client, _ = test_api_client_persistent
    current_data = event_data_response.json()
    assert current_data.pop("status") == EventDataStatus.DRAFT
    assert current_data.pop("updated_at") is None

    response = test_api_client.patch(
        f"/event_data/{current_data['_id']}", json={"status": EventDataStatus.PUBLISHED}
    )
    assert response.status_code == HTTPStatus.OK
    updated_data = response.json()
    updated_at = datetime.datetime.fromisoformat(updated_data.pop("updated_at"))
    assert updated_at > datetime.datetime.fromisoformat(updated_data["created_at"])

    # expect status to be published
    assert updated_data.pop("status") == EventDataStatus.PUBLISHED

    # the other fields should be unchanged
    assert updated_data == current_data
