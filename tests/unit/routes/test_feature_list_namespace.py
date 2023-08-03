"""
Test for FeatureListNamespace route
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestFeatureListNamespaceApi(BaseCatalogApiTestSuite):
    """
    TestFeatureListNamespaceApi
    """

    class_name = "FeatureListNamespace"
    base_route = "/feature_list_namespace"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_multi.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

    @property
    def class_name_to_save(self):
        """Class name used to save the object"""
        return "FeatureList"

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature", "feature_sum_2h"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

    @pytest_asyncio.fixture
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        api_client, _ = test_api_client_persistent
        self.setup_creation_route(api_client)

        feature_list_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_list_multi.json"
        )
        feature_list_response = api_client.post("/feature_list", json=feature_list_payload)
        feature_list_namespace_id = feature_list_response.json()["feature_list_namespace_id"]

        response = api_client.get(f"{self.base_route}/{feature_list_namespace_id}")
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
        create_success_response,
    ):
        """Test creation (success) in default catalog"""

    def multiple_success_payload_generator(self, api_client):
        """Generate multiple success payloads"""
        feature_list_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_list_multi.json"
        )
        for i in range(3):
            feature_list_payload = feature_list_payload.copy()
            feature_list_payload["_id"] = str(ObjectId())
            feature_list_payload["name"] = f'{feature_list_payload["name"]}_{i}'
            yield feature_list_payload

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        output = []
        for feature_list_payload in self.multiple_success_payload_generator(test_api_client):
            feature_list_response = test_api_client.post("/feature_list", json=feature_list_payload)
            feature_list_namespace_id = feature_list_response.json()["feature_list_namespace_id"]
            response = test_api_client.get(f"{self.base_route}/{feature_list_namespace_id}")
            assert response.status_code == HTTPStatus.OK
            output.append(response)
        return output

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict == {
            "name": "sf_feature_list_multiple",
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"}
            ],
            "primary_entity": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "grocery"}
            ],
            "feature_namespace_ids": create_response_dict["feature_namespace_ids"],
            "default_feature_ids": response_dict["default_feature_ids"],
            "default_version_mode": "AUTO",
            "default_feature_list_id": response_dict["default_feature_list_id"],
            "dtype_distribution": [{"count": 2, "dtype": "FLOAT"}],
            "version_count": 1,
            "feature_count": 2,
            "status": "DRAFT",
            "catalog_name": "grocery",
            "description": None,
        }

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert verbose_response.status_code == HTTPStatus.OK, verbose_response.text
