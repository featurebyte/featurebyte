"""
Test for FeatureNamespace route
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestFeatureNamespaceApi(BaseCatalogApiTestSuite):
    """
    TestFeatureNamespaceApi
    """

    class_name = "FeatureNamespace"
    base_route = "/feature_namespace"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_sum_30m.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

    @property
    def class_name_to_save(self):
        """Class name used to save the object"""
        return "Feature"

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        for i in range(3):
            feature_payload = feature_payload.copy()
            feature_payload["_id"] = str(ObjectId())
            feature_payload["name"] = f'{feature_payload["name"]}_{i}'
            yield feature_payload

    @pytest_asyncio.fixture
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_response = test_api_client.post("/feature", json=feature_payload)
        feature_namespace_id = feature_response.json()["feature_namespace_id"]

        response = test_api_client.get(f"{self.base_route}/{feature_namespace_id}")
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

    def post_payloads(self, api_client, api_object_filename_pairs, catalog_id):
        """Post payloads from the fixture requests"""
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            headers = {"active-catalog-id": str(catalog_id)} if catalog_id else None
            response = api_client.post(f"/{api_object}", headers=headers, json=payload)
            assert response.status_code == HTTPStatus.CREATED

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        output = []
        for feature_payload in self.multiple_success_payload_generator(test_api_client):
            feature_response = test_api_client.post("/feature", json=feature_payload)
            feature_namespace_id = feature_response.json()["feature_namespace_id"]
            response = test_api_client.get(f"{self.base_route}/{feature_namespace_id}")
            assert response.status_code == HTTPStatus.OK
            output.append(response)
        return output

    def _create_feature_namespace_with_manual_version_mode(self, test_api_client):
        """Create feature namespace with manual version mode"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
        ]
        self.post_payloads(test_api_client, api_object_filename_pairs, None)
        payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_id = payload["_id"]
        feature_response = test_api_client.get(f"/feature/{feature_id}")
        doc_id = feature_response.json()["feature_namespace_id"]
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
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
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
            "Cannot set default feature ID when default version mode is not MANUAL."
        )

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
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
            "primary_entity": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "default"}
            ],
            "primary_table": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "default"}
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
        self, test_api_client_persistent, catalog_id
    ):
        """Create object with non default catalog"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client, catalog_id=catalog_id)

        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_response = test_api_client.post(
            "/feature", headers={"active-catalog-id": str(catalog_id)}, json=feature_payload
        )
        feature_namespace_id = feature_response.json()["feature_namespace_id"]

        response = test_api_client.get(
            f"{self.base_route}/{feature_namespace_id}",
            headers={"active-catalog-id": str(catalog_id)},
        )
        assert response.status_code == HTTPStatus.OK
        return response
