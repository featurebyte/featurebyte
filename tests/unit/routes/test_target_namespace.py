"""
Test for target namespace routes
"""

from http import HTTPStatus

from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestTargetNamespaceApi(BaseCatalogApiTestSuite):
    """
    TestTargetNamespaceApi class
    """

    class_name = "TargetNamespace"
    base_route = "/target_namespace"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/target_namespace.json"
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
            f'TargetNamespace (id: "{unknown_id}") not found. Please save the TargetNamespace object first.',
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
        target_payload = self.load_payload("tests/fixtures/request_payloads/target_namespace.json")
        _ = api_client
        for i in range(3):
            target_payload = target_payload.copy()
            target_payload["_id"] = str(ObjectId())
            target_payload["name"] = f'{target_payload["name"]}_{i}'
            yield target_payload

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test create target namespace"""
        test_api_client, _ = test_api_client_persistent
        response = create_success_response
        assert response.status_code == HTTPStatus.CREATED, response.json()
        assert response.json()["target_type"] == "regression"

    def test_update_422_setting_target_type(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update target namespace"""
        test_api_client, _ = test_api_client_persistent
        target_namespace_id = create_success_response.json()["_id"]
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}", json={"target_type": "classification"}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"] == "Updating target type after setting it is not supported."
        )

    def test_update_target_type_200(self, test_api_client_persistent):
        """Test update target type"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(
            "/target_namespace", json={**self.payload, "target_type": None}
        )
        target_namespace_id = response.json()["_id"]
        assert response.status_code == HTTPStatus.CREATED, response.json()
        assert response.json()["target_type"] is None

        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}", json={"target_type": "classification"}
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["target_type"] == "classification"

    def test_delete_target_namespace(self, test_api_client_persistent, create_success_response):
        """Test delete target namespace"""
        test_api_client, _ = test_api_client_persistent
        target_namespace_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"/target_namespace/{target_namespace_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

    def test_delete_target_namespace_referenced_in_use_case(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete target namespace referenced in use case"""
        test_api_client, _ = test_api_client_persistent
        target_namespace_id = create_success_response.json()["_id"]
        api_object_filename_pairs = [
            ("context", "context"),
            ("use_case", "use_case"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            if api_object == "use_case":
                payload["target_namespace_id"] = target_namespace_id
                payload["target_id"] = None
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

        response = test_api_client.delete(f"/target_namespace/{target_namespace_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"] == "TargetNamespace is referenced by UseCase: test_use_case "
        )
