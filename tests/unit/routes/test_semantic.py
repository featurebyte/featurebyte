"""
Tests for Semantic route
"""
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
