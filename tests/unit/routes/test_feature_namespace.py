"""
Test for FeatureNamespace route
"""
import time
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest
import pytest_asyncio
from bson import ObjectId
from requests import Response

from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate
from featurebyte.service.feature_namespace import FeatureNamespaceService
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureNamespaceApi(BaseApiTestSuite):
    """
    TestFeatureNamespaceApi
    """

    class_name = "FeatureNamespace"
    base_route = "/feature_namespace"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_namespace.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
            yield mock

    @property
    def class_name_to_save(self):
        """Class name used to save the object"""
        return "Feature"

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    @pytest_asyncio.fixture
    async def create_success_response(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post route success response object"""
        _, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_namespace_service = FeatureNamespaceService(user=user, persistent=persistent)
        document = await feature_namespace_service.create_document(
            data=FeatureNamespaceCreate(**self.payload)
        )
        response = Response()
        response._content = bytes(document.json(by_alias=True), "utf-8")
        response.status_code = HTTPStatus.CREATED
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

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post multiple success responses"""
        test_api_client, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_namespace_service = FeatureNamespaceService(user=user, persistent=persistent)
        output = []
        for i, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
            payload["name"] = f'{self.payload["name"]}_{i}'
            document = await feature_namespace_service.create_document(
                data=FeatureNamespaceCreate(**payload)
            )
            output.append(document)
            time.sleep(0.05)
        return output

    async def setup_get_info(self, api_client, persistent, user_id):
        """Setup for get_info route testing"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

        payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        await persistent.insert_one(
            collection_name="feature",
            document=FeatureCreate(**payload).dict(by_alias=True),
            user_id=user_id,
        )

    def test_update_200(self, test_api_client_persistent):
        """Test update (success)"""
        test_api_client, _ = test_api_client_persistent
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

        payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        doc_id = payload["feature_namespace_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"default_version_mode": "MANUAL"}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["default_version_mode"] == "MANUAL"
        assert response_dict["readiness"] == "DRAFT"

        # upgrade default feature_sum_30m to production ready & check the default feature readiness get updated
        feature_id = response_dict["default_feature_id"]
        response = test_api_client.patch(
            f"feature/{feature_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response_dict["readiness"] == "PRODUCTION_READY"

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response, user_id):
        """Test retrieve info"""
        test_api_client, persistent = test_api_client_persistent
        create_response_dict = create_success_response.json()
        await self.setup_get_info(test_api_client, persistent, user_id)
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )

        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict == {
            "name": "sum_30m",
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "entities": [{"name": "customer", "serving_names": ["cust_id"]}],
            "event_data": [{"name": "sf_event_data", "status": "DRAFT"}],
            "default_version_mode": "AUTO",
            "default_feature_id": response_dict["default_feature_id"],
            "dtype": "FLOAT",
            "version_count": 1,
        }

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert verbose_response.status_code == HTTPStatus.OK, verbose_response.text
