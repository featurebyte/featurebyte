"""
Tests for BatchFeatureTable routes
"""
import copy
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseMaterializedTableTestSuite


class TestBatchFeatureTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for BatchFeatureTable route
    """

    class_name = "BatchFeatureTable"
    base_route = "/batch_feature_table"
    payload = BaseMaterializedTableTestSuite.load_payload(
        "tests/fixtures/request_payloads/batch_feature_table.json"
    )
    random_id = str(ObjectId())

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'BatchFeatureTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `BatchFeatureTable.get(name="{payload["name"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'BatchFeatureTable (name: "{payload["name"]}") already exists. '
            f'Get the existing object by `BatchFeatureTable.get(name="{payload["name"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "random_name",
                "batch_request_table_id": random_id,
            },
            f'BatchRequestTable (id: "{random_id}") not found. Please save the BatchRequestTable object first.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "random_name", "deployment_id": random_id},
            f'Deployment (id: "{random_id}") not found. Please save the Deployment object first.',
        ),
    ]

    @pytest.fixture(autouse=True)
    def mock_online_enable_service_update_data_warehouse(self):
        """Mock update_data_warehouse method in OnlineEnableService to make it a no-op"""
        with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
            yield

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("context", "context"),
            ("batch_request_table", "batch_request_table"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
            ("deployment", "deployment"),
        ]
        catalog_id = api_client.get("/catalog").json()["data"][0]["_id"]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            if api_object in {"batch_request_table", "deployment"}:
                response = self.wait_for_results(api_client, response)
                assert response.json()["status"] == "SUCCESS"
            else:
                assert response.status_code == HTTPStatus.CREATED

            if api_object == "feature":
                self.make_feature_production_ready(api_client, response.json()["_id"], catalog_id)
            if api_object == "deployment":
                assert response.json()["status"] == "SUCCESS"
                deployment_id = response.json()["payload"]["output_document_id"]
                self.update_deployment_enabled(api_client, deployment_id, catalog_id)

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def test_create_422__batch_request_table_failed_validation_check(
        self,
        test_api_client_persistent,
        snowflake_execute_query_invalid_batch_request_table,
    ):
        """Test create 422 for batch request table failed validation check"""
        _ = snowflake_execute_query_invalid_batch_request_table
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # check that columns_info is empty as we are mocking the query
        batch_request_table_id = self.payload["batch_request_table_id"]
        response = test_api_client.get(f"/batch_request_table/{batch_request_table_id}")
        assert response.json()["columns_info"] == []

        # check that create fails
        response = test_api_client.post(self.base_route, json=self.payload)

        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            'Required entities are not provided in the request: customer (serving name: "cust_id")'
        )

    @pytest.mark.asyncio
    async def test_batch_request_table_delete_422__batch_request_table_failed_validation_check(
        self, test_api_client_persistent, create_success_response, user_id, default_catalog_id
    ):
        """Test delete 422 for batch request table failed validation check"""
        test_api_client, persistent = test_api_client_persistent
        create_success_response_dict = create_success_response.json()
        batch_feature_table_id = create_success_response_dict["_id"]

        # insert another document to batch feature table to make sure the query filter is correct
        payload = copy.deepcopy(self.payload)
        payload["_id"] = ObjectId()
        payload["name"] = "random_name"
        await persistent.insert_one(
            collection_name="batch_feature_table",
            document={
                **payload,
                "catalog_id": ObjectId(default_catalog_id),
                "user_id": user_id,
                "batch_request_table_id": ObjectId(),  # different batch request table id
                "columns_info": [],
                "num_rows": 500,
                "location": create_success_response_dict["location"],
            },
            user_id=user_id,
        )
        response = test_api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["total"] == 2

        # try to delete batch request table
        batch_request_table_id = self.payload["batch_request_table_id"]
        response = test_api_client.delete(f"/batch_request_table/{batch_request_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            f"Cannot delete Batch Request Table {batch_request_table_id} because it is referenced by "
            f"1 Batch Feature Table(s): ['{batch_feature_table_id}']"
        )

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": self.payload["name"],
            "deployment_name": "my_deployment",
            "batch_request_table_name": "batch_request_table",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "description": None,
        }
