"""
Tests for Semantic route
"""

from http import HTTPStatus

from bson.objectid import ObjectId

from tests.unit.routes.base import BaseRelationshipApiTestSuite


class TestSemanticApi(BaseRelationshipApiTestSuite):
    """
    TestSemanticApi class
    """

    class_name = "Semantic"
    base_route = "/semantic"
    unknown_id = ObjectId()
    semantic_id = ObjectId()
    payload = {"_id": str(semantic_id), "name": "EVENT_TIMESTAMP"}
    semantic_names = []
    quoted_semantic_names = []
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'Semantic (id: "{payload["_id"]}") already exists.')
    ]
    create_unprocessable_payload_expected_detail_pairs = []
    create_parent_unprocessable_payload_expected_detail_pairs = [
        (
            {"id": str(unknown_id)},
            f'Semantic (id: "{unknown_id}") not found. Please save the Semantic object first.',
        )
    ]

    def multiple_success_payload_generator(self, api_client):
        """Post multiple success responses"""
        _ = api_client
        semantic_names = ["EVENT_ID", "EVENT_TIMESTAMP", "ITEM_ID"]
        for semantic in semantic_names:
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = semantic
            yield payload

    def test_delete(self, create_success_response, test_api_client_persistent):
        """Test delete"""
        test_api_client, _ = test_api_client_persistent

        semantic_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"{self.base_route}/{semantic_id}")
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(f"{self.base_route}/{semantic_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND
