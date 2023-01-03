"""
Tests for Context route
"""
from http import HTTPStatus

from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestContextApi(BaseApiTestSuite):
    """
    TestContextApi class
    """

    class_name = "Context"
    base_route = "/context"
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/context.json")
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'Context (id: "{payload["_id"]}") already exists.')
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {"name": "some_context"},
            [
                {
                    "loc": ["body", "entity_ids"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
        )
    ]

    def multiple_success_payload_generator(self, api_client):
        """Payload generator to create multiple success response"""
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f"{payload['name']}_{i}"
            yield payload

    def test_update_200(self, create_success_response, test_api_client_persistent):
        """
        Test context update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]
        graph = {"nodes": [], "edges": []}
        response = test_api_client.patch(f"{self.base_route}/{context_id}", json={"graph": graph})
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["graph"] == graph

    def test_update_404(self, test_api_client_persistent):
        """
        Test context update (not found)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_context_id = ObjectId()
        response = test_api_client.patch(
            f"{self.base_route}/{unknown_context_id}", json={"name": "random_name"}
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {
            "detail": (
                f'Context (id: "{unknown_context_id}") not found. Please save the Context object first.'
            )
        }
