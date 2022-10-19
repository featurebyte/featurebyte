"""
Tests for EventData routes
"""
import datetime
from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import DataStatus
from featurebyte.schema.event_data import EventDataCreate
from tests.unit.routes.base import BaseDataApiTestSuite


class TestEventDataApi(BaseDataApiTestSuite):
    """
    TestEventDataApi class
    """

    class_name = "EventData"
    base_route = "/event_data"
    data_create_schema_class = EventDataCreate
    payload = BaseDataApiTestSuite.load_payload("tests/fixtures/request_payloads/event_data.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'EventData (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `EventData.get(name="sf_event_data")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'EventData (name: "sf_event_data") already exists. '
            'Get the existing object by `EventData.get(name="sf_event_data")`.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "other_name"},
            f"EventData (tabular_source: \"{{'feature_store_id': "
            f'ObjectId(\'{payload["tabular_source"]["feature_store_id"]}\'), \'table_details\': '
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'sf_table'}}\") "
            'already exists. Get the existing object by `EventData.get(name="sf_event_data")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "tabular_source": ("Some other source", "other table")},
            [
                {
                    "ctx": {"object_type": "TabularSource"},
                    "loc": ["body", "tabular_source"],
                    "msg": "value is not a valid TabularSource type",
                    "type": "type_error.featurebytetype",
                }
            ],
        )
    ]

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(self, tabular_source, columns_info, user_id):
        """Fixture for a Event Data dict"""
        event_data_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": columns_info,
            "event_id_column": "event_id",
            "event_timestamp_column": "event_date",
            "record_creation_date_column": "created_at",
            "default_feature_job_setting": {
                "blind_spot": "10m",
                "frequency": "30m",
                "time_modulo_frequency": "5m",
            },
            "status": "PUBLISHED",
            "user_id": str(user_id),
        }
        output = EventDataModel(**event_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Event data update dict object
        """
        return {
            "default_feature_job_setting": {
                "blind_spot": "12m",
                "frequency": "30m",
                "time_modulo_frequency": "5m",
            },
            "record_creation_date_column": "created_at",
        }

    def test_update_success(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
        data_model_dict,
    ):
        """
        Update Event Data
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]

        response = test_api_client.patch(f"{self.base_route}/{insert_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.OK
        update_response_dict = response.json()
        assert update_response_dict["_id"] == insert_id
        update_response_dict.pop("created_at")
        update_response_dict.pop("updated_at")

        # default_feature_job_setting should be updated
        assert (
            update_response_dict.pop("default_feature_job_setting")
            == data_update_dict["default_feature_job_setting"]
        )

        # the other fields should be unchanged
        data_model_dict.pop("default_feature_job_setting")
        data_model_dict["status"] = DataStatus.DRAFT
        assert update_response_dict == data_model_dict

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

    def test_update_record_creation_date(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
    ):
        """
        Update Event Data record creation date column
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]

        update_response = test_api_client.patch(
            f"/event_data/{insert_id}",
            json={**data_update_dict, "record_creation_date_column": "another_created_at"},
        )
        update_response_dict = update_response.json()
        expected_response = {
            **response_dict,
            **data_update_dict,
            "record_creation_date_column": "another_created_at",
        }
        expected_response.pop("updated_at")
        assert update_response_dict.items() > expected_response.items()
        assert update_response_dict["updated_at"] is not None

    def test_update_record_creation_date__column_not_exists(
        self, test_api_client_persistent, data_response
    ):
        """
        Update Event Data record creation date column (when the column does not exist)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]

        update_response = test_api_client.patch(
            f"/event_data/{insert_id}",
            json={"record_creation_date_column": "non-exist-columns"},
        )
        update_response_dict = update_response.json()
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert update_response_dict["detail"] == (
            "1 validation error for EventDataModel\n"
            "record_creation_date_column\n  "
            'Column "non-exist-columns" not found in the table! (type=value_error)'
        )

    def test_update_excludes_unsupported_fields(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
        data_model_dict,
    ):
        """
        Update Event Data only updates job settings even if other fields are provided
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]
        assert insert_id

        # expect status to be draft
        assert response_dict["status"] == DataStatus.DRAFT

        data_update_dict["name"] = "Some other name"
        data_update_dict["source"] = "Some other source"
        data_update_dict["status"] = DataStatus.PUBLISHED.value
        response = test_api_client.patch(f"/event_data/{insert_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["_id"] == insert_id
        data.pop("created_at")
        data.pop("updated_at")

        # default_feature_job_setting should be updated
        assert (
            data.pop("default_feature_job_setting")
            == data_update_dict["default_feature_job_setting"]
        )

        # the other fields should be unchanged
        data_model_dict.pop("default_feature_job_setting")
        assert data == data_model_dict

        # expect status to be updated to published
        assert data["status"] == DataStatus.PUBLISHED

    def test_get_default_feature_job_setting_history(
        self, test_api_client_persistent, data_response
    ):
        """
        Test retrieve default feature job settings history
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
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

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        expected_info_response = {
            "name": "sf_event_data",
            "updated_at": None,
            "event_timestamp_column": "event_timestamp",
            "record_creation_date_column": "created_at",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "default_feature_job_setting": None,
            "status": "DRAFT",
            "entities": [{"name": "customer", "serving_names": ["cust_id"]}],
            "column_count": 9,
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        verbose_response_dict = verbose_response.json()
        assert verbose_response_dict.items() > expected_info_response.items(), verbose_response.text
        assert "created_at" in verbose_response_dict
        assert verbose_response_dict["columns_info"] is not None
