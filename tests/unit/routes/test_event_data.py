"""
Tests for EventData routes
"""
import datetime
from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.models.event_data import EventDataModel, EventDataStatus
from tests.unit.routes.base import BaseApiTestSuite


class TestEventDataApi(BaseApiTestSuite):
    """
    TestEventDataApi class
    """

    class_name = "EventData"
    base_route = "/event_data"
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/event_data.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'EventData (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `EventData.get(name="sf_event_data")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'EventData (name: "sf_event_data") already exists. '
            f'Get the existing object by `EventData.get(name="sf_event_data")`.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "other_name"},
            f'EventData (tabular_source: "(ObjectId(\'{payload["tabular_source"][0]}\'), '
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'sf_table'})\") "
            'already exists. Get the existing object by `EventData.get(name="sf_event_data")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "tabular_source": ("Some other source", "other table")},
            [
                {
                    "loc": ["body", "tabular_source", 0],
                    "msg": "Id must be of type PydanticObjectId",
                    "type": "type_error",
                },
                {
                    "ctx": {"object_type": "TableDetails"},
                    "loc": ["body", "tabular_source", 1],
                    "msg": "value is not a valid TableDetails type",
                    "type": "type_error.featurebytetype",
                },
            ],
        )
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        # save feature store
        feature_store_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_store.json"
        )
        response = api_client.post("/feature_store", json=feature_store_payload)
        assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            feature_store_id, table_details = payload["tabular_source"]
            payload["tabular_source"] = [
                feature_store_id,
                {key: f"{value}_{i}" for key, value in table_details.items()},
            ]
            yield payload

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        assert create_success_response.json()["status"] == EventDataStatus.DRAFT

    @pytest.fixture(name="event_data_model_dict")
    def event_data_model_dict_fixture(self, snowflake_feature_store, user_id):
        """Fixture for a Event Data dict"""
        event_data_dict = {
            "name": "订单表",
            "tabular_source": [
                str(snowflake_feature_store.id),
                {
                    "database_name": "database",
                    "schema_name": "schema",
                    "table_name": "table",
                },
            ],
            "event_timestamp_column": "event_date",
            "record_creation_date_column": "created_at",
            "column_entity_map": None,
            "default_feature_job_setting": {
                "blind_spot": "10m",
                "frequency": "30m",
                "time_modulo_frequency": "5m",
            },
            "history": [
                {
                    "created_at": datetime.datetime(2022, 4, 1),
                    "setting": {
                        "blind_spot": "10m",
                        "frequency": "30m",
                        "time_modulo_frequency": "5m",
                    },
                },
                {
                    "created_at": datetime.datetime(2022, 2, 1),
                    "setting": {
                        "blind_spot": "10m",
                        "frequency": "30m",
                        "time_modulo_frequency": "5m",
                    },
                },
            ],
            "status": "PUBLISHED",
            "user_id": str(user_id),
        }
        output = EventDataModel(**event_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="event_data_update_dict")
    def event_data_update_dict_fixture(self):
        """
        Table Event update dict object
        """
        return {
            "default_feature_job_setting": {
                "blind_spot": "12m",
                "frequency": "30m",
                "time_modulo_frequency": "5m",
            },
            "status": "DRAFT",
            "record_creation_date_column": "created_at",
        }

    @pytest.fixture(name="event_data_response")
    def event_data_response_fixture(
        self, test_api_client_persistent, event_data_model_dict, snowflake_feature_store
    ):
        """
        Event data response fixture
        """
        test_api_client, persistent = test_api_client_persistent
        snowflake_feature_store.save()
        response = test_api_client.post(
            "/event_data", json=EventDataModel(**event_data_model_dict).json_dict()
        )
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["_id"] == event_data_model_dict["_id"]
        return response

    def test_update_success(
        self,
        test_api_client_persistent,
        event_data_response,
        event_data_update_dict,
        event_data_model_dict,
    ):
        """
        Update Event Data
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = event_data_response.json()
        insert_id = response_dict["_id"]

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
        event_data_model_dict["status"] = EventDataStatus.DRAFT
        assert update_response_dict == event_data_model_dict

        # test get audit records
        response = test_api_client.get(f"/event_data/audit/{insert_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        assert [
            record["previous_values"].get("default_feature_job_setting")
            for record in results["data"]
        ] == [{"blind_spot": "10m", "frequency": "30m", "time_modulo_frequency": "5m"}, None]

        # test get default_feature_job_setting_history
        response = test_api_client.get(
            f"/event_data/history/default_feature_job_setting/{insert_id}"
        )
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert [doc["setting"] for doc in results] == [
            {"blind_spot": "12m", "frequency": "30m", "time_modulo_frequency": "5m"},
            {"blind_spot": "10m", "frequency": "30m", "time_modulo_frequency": "5m"},
        ]

    def test_update_fails_table_not_found(self, test_api_client_persistent, event_data_update_dict):
        """
        Update Event Data fails if table not found
        """
        test_api_client, _ = test_api_client_persistent
        random_id = ObjectId()
        response = test_api_client.patch(f"/event_data/{random_id}", json=event_data_update_dict)
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {
            "detail": f'EventData (id: "{random_id}") not found. Please save the EventData object first.'
        }

    def test_update_excludes_unsupported_fields(
        self,
        test_api_client_persistent,
        event_data_response,
        event_data_update_dict,
        event_data_model_dict,
    ):
        """
        Update Event Data only updates job settings even if other fields are provided
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = event_data_response.json()
        insert_id = response_dict["_id"]
        assert insert_id

        # expect status to be draft
        assert response_dict["status"] == EventDataStatus.DRAFT

        event_data_update_dict["name"] = "Some other name"
        event_data_update_dict["source"] = "Some other source"
        event_data_update_dict["status"] = EventDataStatus.PUBLISHED.value
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
        assert data == event_data_model_dict

        # expect status to be updated to published
        assert data["status"] == EventDataStatus.PUBLISHED

    def test_update_fails_invalid_transition(
        self, test_api_client_persistent, event_data_response, event_data_update_dict
    ):
        """
        Update Event Data fails if status transition is no valid
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = event_data_response.json()
        event_data_update_dict["status"] = EventDataStatus.DEPRECATED.value
        response = test_api_client.patch(
            f"/event_data/{response_dict['_id']}", json=event_data_update_dict
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {"detail": "Invalid status transition from DRAFT to DEPRECATED."}

    def test_update_status_only(self, test_api_client_persistent, event_data_response):
        """
        Update Event Data status only
        """
        # insert a record
        test_api_client, _ = test_api_client_persistent
        current_data = event_data_response.json()
        assert current_data.pop("status") == EventDataStatus.DRAFT
        assert current_data.pop("updated_at") is None

        response = test_api_client.patch(
            f"/event_data/{current_data['_id']}",
            json={**current_data, "status": EventDataStatus.PUBLISHED.value},
        )
        assert response.status_code == HTTPStatus.OK
        updated_data = response.json()
        updated_at = datetime.datetime.fromisoformat(updated_data.pop("updated_at"))
        assert updated_at > datetime.datetime.fromisoformat(updated_data["created_at"])

        # expect status to be published
        assert updated_data.pop("status") == EventDataStatus.PUBLISHED

        # the other fields should be unchanged
        assert updated_data == current_data

        # test get audit records
        response = test_api_client.get(f"/event_data/audit/{current_data['_id']}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        assert [record["previous_values"].get("status") for record in results["data"]] == [
            "DRAFT",
            None,
        ]

    def test_get_default_feature_job_setting_history(
        self, test_api_client_persistent, event_data_response
    ):
        """
        Test retrieve default feature job settings history
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = event_data_response.json()
        document_id = response_dict["_id"]
        expected_history = [
            {
                "created_at": response_dict["created_at"],
                "setting": response_dict["default_feature_job_setting"],
            }
        ]

        for blind_spot in ["1m", "3m", "5m", "10m", "12m"]:
            response = test_api_client.patch(
                f"/event_data/{document_id}",
                json={
                    "default_feature_job_setting": {
                        "blind_spot": blind_spot,
                        "frequency": "30m",
                        "time_modulo_frequency": "5m",
                    },
                    "status": "DRAFT",
                },
            )
            assert response.status_code == HTTPStatus.OK
            update_response_dict = response.json()
            expected_history.append(
                {
                    "created_at": update_response_dict["updated_at"],
                    "setting": update_response_dict["default_feature_job_setting"],
                }
            )

        # test get default_feature_job_setting_history
        response = test_api_client.get(
            f"/event_data/history/default_feature_job_setting/{document_id}"
        )
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert list(reversed(results)) == expected_history
