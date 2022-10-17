"""
Tests for Semantic route
"""
from bson.objectid import ObjectId

from featurebyte.models.semantic import SemanticName
from tests.unit.routes.base import BaseRelationshipApiTestSuite


class TestSemanticApi(BaseRelationshipApiTestSuite):
    """
    TestSemanticApi class
    """

    class_name = "Semantic"
    base_route = "/semantic"
    unknown_id = ObjectId()
    semantic_id = ObjectId()
    payload = {"_id": str(semantic_id), "name": SemanticName.EVENT_TIMESTAMP.value}
    semantic_names = [semantic.value for semantic in SemanticName]
    quoted_semantic_names = [f"'{name}'" for name in semantic_names]
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'Semantic (id: "{payload["_id"]}") already exists.')
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "name": "random_name"},
            [
                {
                    "loc": ["body", "name"],
                    "msg": f"value is not a valid enumeration member; permitted: {', '.join(quoted_semantic_names)}",
                    "type": "type_error.enum",
                    "ctx": {"enum_values": semantic_names},
                }
            ],
        )
    ]
    create_parent_unprocessable_payload_expected_detail_pairs = [
        (
            {"id": str(unknown_id)},
            f'Semantic (id: "{unknown_id}") not found. Please save the Semantic object first.',
        )
    ]

    def multiple_success_payload_generator(self, api_client):
        """Post multiple success responses"""
        _ = api_client
        for i, semantic in enumerate(SemanticName):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = semantic.value
            yield payload

            if i == 2:
                break
