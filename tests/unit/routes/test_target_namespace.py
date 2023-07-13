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
