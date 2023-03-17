"""
Test for FeatureNamespace route
"""
import time
from http import HTTPStatus
from unittest.mock import Mock

import pytest
import pytest_asyncio
from bson import ObjectId
from requests import Response

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.schema.feature import FeatureCreate
from featurebyte.schema.feature_namespace import FeatureNamespaceCreate
from featurebyte.service.feature_namespace import FeatureNamespaceService
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestFeatureNamespaceApi(BaseCatalogApiTestSuite):
    """
    TestFeatureNamespaceApi
    """

    class_name = "FeatureNamespace"
    base_route = "/feature_namespace"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_namespace.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

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
        feature_namespace_service = FeatureNamespaceService(
            user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
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

    @pytest.mark.skip("POST method not exposed")
    def test_create_201_non_default_catalog(
        self,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test creation (success) in non default catalog"""

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post multiple success responses"""
        test_api_client, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_namespace_service = FeatureNamespaceService(
            user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
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
            ("event_table", "event_table"),
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

    def _create_feature_namespace_with_manual_version_mode(self, test_api_client):
        """Create feature namespace with manual version mode"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
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
        return response_dict

    def test_update_200__update_readiness_on_manual_mode(self, test_api_client_persistent):
        """Test update (success): update readiness on manual mode"""
        test_api_client, _ = test_api_client_persistent
        response_dict = self._create_feature_namespace_with_manual_version_mode(test_api_client)

        # upgrade default feature_sum_30m to production ready & check the default feature readiness get updated
        doc_id = response_dict["_id"]
        feature_id = response_dict["default_feature_id"]
        response = test_api_client.patch(
            f"feature/{feature_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response_dict["readiness"] == "PRODUCTION_READY"

    def test_update__set_default_feature_id_on_manual_mode(self, test_api_client_persistent):
        """Test update: set default feature id on manual mode"""
        test_api_client, _ = test_api_client_persistent
        response_dict = self._create_feature_namespace_with_manual_version_mode(test_api_client)
        feature_namespace_id = response_dict["_id"]
        feature_id = response_dict["default_feature_id"]

        # update current default feature to production ready
        test_api_client.patch(f"feature/{feature_id}", json={"readiness": "PRODUCTION_READY"})

        # create a new feature and set it as default feature
        post_feature_response = test_api_client.post(
            "/feature",
            json={
                "source_feature_id": feature_id,
                "data_feature_job_settings": [
                    {
                        "data_name": "sf_event_data",
                        "feature_job_setting": {
                            "blind_spot": "23h",
                            "frequency": "24h",
                            "time_modulo_frequency": "1h",
                        },
                    }
                ],
            },
        )
        new_feature_id = post_feature_response.json()["_id"]

        # check feature namespace response (make sure new feature ID is included)
        namespace_response = test_api_client.get(f"{self.base_route}/{feature_namespace_id}")
        namespace_response_dict = namespace_response.json()
        assert namespace_response_dict["default_version_mode"] == "MANUAL"
        assert namespace_response_dict["feature_ids"] == [feature_id, new_feature_id]
        assert namespace_response_dict["default_feature_id"] == feature_id
        assert namespace_response_dict["readiness"] == "PRODUCTION_READY"

        # test update new default feature ID
        update_response = test_api_client.patch(
            f"{self.base_route}/{feature_namespace_id}", json={"default_feature_id": new_feature_id}
        )
        update_response_dict = update_response.json()
        assert update_response.status_code == HTTPStatus.OK
        assert namespace_response_dict["feature_ids"] == [feature_id, new_feature_id]
        assert update_response_dict["default_feature_id"] == new_feature_id
        assert update_response_dict["readiness"] == "DRAFT"

        # check update version mode back to AUTO and the default feature id get updated
        update_response = test_api_client.patch(
            f"{self.base_route}/{feature_namespace_id}", json={"default_version_mode": "AUTO"}
        )
        update_response_dict = update_response.json()
        assert update_response.status_code == HTTPStatus.OK
        assert update_response_dict["default_version_mode"] == "AUTO"
        assert update_response_dict["feature_ids"] == [feature_id, new_feature_id]
        assert update_response_dict["default_feature_id"] == feature_id
        assert update_response_dict["readiness"] == "PRODUCTION_READY"

        # check update default feature ID on AUTO mode
        update_response = test_api_client.patch(
            f"{self.base_route}/{feature_namespace_id}", json={"default_feature_id": new_feature_id}
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert update_response.json()["detail"] == (
            "Cannot set default feature ID when default version mode is not MANUAL"
        )

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
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}
            ],
            "tabular_data": [
                {"name": "sf_event_data", "status": "DRAFT", "catalog_name": "default"}
            ],
            "default_version_mode": "AUTO",
            "default_feature_id": response_dict["default_feature_id"],
            "dtype": "FLOAT",
            "version_count": 1,
            "catalog_name": "default",
        }

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert verbose_response.status_code == HTTPStatus.OK, verbose_response.text

    @pytest_asyncio.fixture
    async def create_success_response_non_default_catalog(
        self, test_api_client_persistent, user_id, catalog_id
    ):  # pylint: disable=arguments-differ
        """Create object with non default catalog"""
        _, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_namespace_service = FeatureNamespaceService(
            user=user, persistent=persistent, catalog_id=catalog_id
        )
        document = await feature_namespace_service.create_document(
            data=FeatureNamespaceCreate(**self.payload)
        )
        response = Response()
        response._content = bytes(document.json(by_alias=True), "utf-8")
        response.status_code = HTTPStatus.CREATED
        return response
