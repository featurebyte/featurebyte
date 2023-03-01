"""
Test relationship info routes
"""
from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.schema.relationship_info import RelationshipInfoUpdate
from tests.unit.routes.base import BaseWorkspaceApiTestSuite


class TestRelationshipInfoApi(BaseWorkspaceApiTestSuite):
    """
    Test relationship info routes
    """

    class_name = "RelationshipInfo"
    base_route = "/relationship_info"
    payload = BaseWorkspaceApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/relationship_info.json"
    )

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["primary_entity_id"] = str(ObjectId())
            payload["related_entity_id"] = str(ObjectId())
            payload["primary_data_source_id"] = str(ObjectId())
            payload["updated_by"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent

        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("event_data", "event_data"),
            ("entity", "entity"),
            ("entity", "transaction_entity"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

        # Create relationship info
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # Get info
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict["relationship_type"] == "child_parent"
        assert response_dict["primary_entity_name"] == "customer"
        assert response_dict["related_entity_name"] == "transaction"
        assert response_dict["data_source_name"] == "sf_event_data"

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """
        Test patch to update is_enabled status
        """
        # Create RelationshipInfo and verify is_enabled is True
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        assert response_dict["is_enabled"]

        # Update is_enabled to False
        data_update = RelationshipInfoUpdate(is_enabled=False)
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}", json=data_update.dict()
        )
        assert response.status_code == HTTPStatus.OK

        # Verify is_enabled is False
        response = test_api_client.get(f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert not response_dict["is_enabled"]
