"""
Test for exposure namespace routes
"""

from http import HTTPStatus

from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestExposureNamespaceApi(BaseCatalogApiTestSuite):
    """
    TestExposureNamespaceApi class
    """

    class_name = "ExposureNamespace"
    base_route = "/exposure_namespace"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/exposure_namespace.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []
    create_parent_unprocessable_payload_expected_detail_pairs = [
        (
            {
                "id": str(unknown_id),
                "table_type": "event_table",
                "table_id": str(ObjectId()),
            },
            f'ExposureNamespace (id: "{unknown_id}") not found. Please save the ExposureNamespace object first.',
        )
    ]

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
        exposure_payload = self.load_payload(
            "tests/fixtures/request_payloads/exposure_namespace.json"
        )
        _ = api_client
        for i in range(3):
            exposure_payload = exposure_payload.copy()
            exposure_payload["_id"] = str(ObjectId())
            exposure_payload["name"] = f"{exposure_payload['name']}_{i}"
            yield exposure_payload

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test create exposure namespace"""
        test_api_client, _ = test_api_client_persistent
        response = create_success_response
        assert response.status_code == HTTPStatus.CREATED, response.json()
        assert response.json()["dtype"] == "FLOAT"
        assert response.json()["window"] == "7d"

    def test_delete_exposure_namespace(self, test_api_client_persistent, create_success_response):
        """Test delete exposure namespace"""
        test_api_client, _ = test_api_client_persistent
        exposure_namespace_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"/exposure_namespace/{exposure_namespace_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

    def test_delete_exposure_namespace_referenced_in_context(self, test_api_client_persistent):
        """Test delete exposure namespace referenced in a context"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # Create an exposure namespace without default_exposure_id
        # so that context creation doesn't try to fetch a non-existent exposure
        exposure_ns_payload = {
            **self.payload,
            "_id": str(ObjectId()),
            "name": "exposure_ns_for_context_test",
            "default_exposure_id": None,
            "exposure_ids": [],
        }
        response = test_api_client.post("/exposure_namespace", json=exposure_ns_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        exposure_namespace_id = response.json()["_id"]

        # Create a context that references the exposure namespace
        context_payload = self.load_payload("tests/fixtures/request_payloads/context.json")
        context_payload["exposure_namespace_id"] = exposure_namespace_id
        context_payload["exposure_id"] = None
        response = test_api_client.post("/context", json=context_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        response = test_api_client.delete(f"/exposure_namespace/{exposure_namespace_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert "ExposureNamespace is referenced by Context" in response.json()["detail"]
