"""
Test for treatment routes
"""

from http import HTTPStatus

from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestTreatmentApi(BaseCatalogApiTestSuite):
    """
    TestTreatmentApi class
    """

    class_name = "Treatment"
    base_route = "/treatment"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/treatment.json")
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []
    create_parent_unprocessable_payload_expected_detail_pairs = []

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        treatment_payload = self.load_payload("tests/fixtures/request_payloads/treatment.json")
        _ = api_client
        for i in range(3):
            treatment_payload = treatment_payload.copy()
            treatment_payload["_id"] = str(ObjectId())
            treatment_payload["name"] = f"{treatment_payload['name']}_{i}"
            yield treatment_payload

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test create treatment"""
        test_api_client, _ = test_api_client_persistent
        response = create_success_response
        assert response.status_code == HTTPStatus.CREATED, response.json()
        assert response.json()["treatment_type"] == "binary"

    def test_delete_treatment(self, test_api_client_persistent, create_success_response):
        """Test delete treatment"""
        test_api_client, _ = test_api_client_persistent
        treatment_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"/treatment/{treatment_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

    def test_delete_treatment_referenced_in_context(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete treatment referenced in use case"""
        test_api_client, _ = test_api_client_persistent
        treatment_id = create_success_response.json()["_id"]
        api_object_filename_pairs = [
            ("context", "context"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            payload["treatment_id"] = treatment_id
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

        response = test_api_client.delete(f"/treatment/{treatment_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"] == "Treatment is referenced by Context: transaction_context"
        )

    def test_create_treatment_with_control_label_invalid_treatment_type(
        self, test_api_client_persistent
    ):
        """Test create treatment with control label but invalid treatment type"""
        test_api_client, _ = test_api_client_persistent
        payload = {
            **self.payload,
            "treatment_type": "numeric",
        }
        response = test_api_client.post("/treatment", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        # Error is returned as a list of validation errors
        detail = response.json()["detail"]
        assert isinstance(detail, list)
        assert any(
            "treatment_labels must be None for numeric treatments" in error.get("msg", "")
            for error in detail
        )
