"""
Tests for BatchRequestTable route
"""

from http import HTTPStatus

from bson import ObjectId

from tests.unit.routes.base import BaseMaterializedTableTestSuite


class TestBatchRequestTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for BatchRequestTable route
    """

    class_name = "BatchRequestTable"
    base_route = "/batch_request_table"
    payload = BaseMaterializedTableTestSuite.load_payload("tests/fixtures/request_payloads/batch_request_table.json")

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'BatchRequestTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `BatchRequestTable.get(name="{payload["name"]}")`.',
        ),
    ]

    unknown_context_id = str(ObjectId())
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "new_table",
                "context_id": unknown_context_id,
            },
            f'Context (id: "{unknown_context_id}") not found. Please save the Context object first.',
        )
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("context", "context"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": self.payload["name"],
            "type": "source_table",
            "feature_store_name": "sf_featurestore",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "description": None,
        }

    def test_delete_context_of_batch_request_table(self, create_success_response, test_api_client_persistent):
        """Test delete context of batch request table"""
        context_id = create_success_response.json()["context_id"]

        test_api_client, _ = test_api_client_persistent
        response = test_api_client.delete(f"/context/{context_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Context is referenced by BatchRequestTable: batch_request_table"
