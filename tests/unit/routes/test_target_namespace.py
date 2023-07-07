"""
Test for target namespace routes
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestTargetNamespaceApi(BaseCatalogApiTestSuite):
    """
    TestTargetNamespaceApi class
    """

    class_name = "TargetNamespace"
    base_route = "/target_namespace"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/target.json")
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        target_payload = self.load_payload("tests/fixtures/request_payloads/target.json")
        _ = api_client
        for i in range(3):
            target_payload = target_payload.copy()
            target_payload["_id"] = str(ObjectId())
            target_payload["name"] = f'{target_payload["name"]}_{i}'
            target_payload["target_namespace_id"] = str(ObjectId())
            yield target_payload

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        output = []
        for target_payload in self.multiple_success_payload_generator(test_api_client):
            target_response = test_api_client.post("/target", json=target_payload)
            target_namespace_id = target_response.json()["target_namespace_id"]
            response = test_api_client.get(f"{self.base_route}/{target_namespace_id}")
            assert response.status_code == HTTPStatus.OK
            output.append(response)
        return output

    @pytest_asyncio.fixture
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        target_payload = self.load_payload("tests/fixtures/request_payloads/target.json")
        target_response = test_api_client.post("/target", json=target_payload)
        target_namespace_id = target_response.json()["target_namespace_id"]

        response = test_api_client.get(f"{self.base_route}/{target_namespace_id}")
        assert response.status_code == HTTPStatus.OK
        return response

    @pytest_asyncio.fixture
    async def create_success_response_non_default_catalog(
        self, test_api_client_persistent, catalog_id
    ):
        """Create object with non default catalog"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client, catalog_id=catalog_id)

        target_payload = self.load_payload("tests/fixtures/request_payloads/target.json")
        target_response = test_api_client.post(
            "/target", headers={"active-catalog-id": str(catalog_id)}, json=target_payload
        )
        target_namespace_id = target_response.json()["target_namespace_id"]

        response = test_api_client.get(
            f"{self.base_route}/{target_namespace_id}",
            headers={"active-catalog-id": str(catalog_id)},
        )
        assert response.status_code == HTTPStatus.OK
        return response

    @pytest.mark.skip("POST method not exposed")
    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        """Test creation (success) without specifying id field"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201__id_is_none(self, test_api_client_persistent):
        """Test creation (success) ID is None"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201_non_default_catalog(
        self,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test creation (success) in non default catalog"""
