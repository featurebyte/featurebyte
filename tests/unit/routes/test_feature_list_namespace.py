"""
Test for FeatureListNamespace route
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature", "feature_sum_2h"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
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
        create_success_response_non_default_catalog,
    ):
        """Test creation (success) in non default catalog"""

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

    def _create_fl_namespace_with_manual_version_mode_and_new_feature_id(self, test_api_client):
        """Create feature list namespace with manual version mode"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
        ]
        feature_id = None
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED
            if api_object == "feature":
                feature_id = response.json()["_id"]

        payload = self.load_payload("tests/fixtures/request_payloads/feature_list_single.json")
        feature_list_id = payload["_id"]
        feature_list_response = test_api_client.get(f"/feature_list/{feature_list_id}")
        feature_list_namespace_id = feature_list_response.json()["feature_list_namespace_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{feature_list_namespace_id}",
            json={"default_version_mode": "MANUAL"},
        )
        response_dict = response.json()
        assert response_dict["default_version_mode"] == "MANUAL"
        assert response_dict["status"] == "DRAFT"
        assert response_dict["readiness_distribution"] == [{"count": 1, "readiness": "DRAFT"}]
        return response_dict, feature_id

    def test_update_200__update_readiness_on_manual_mode(self, test_api_client_persistent):
        """Test update (success): update readiness on manual mode"""
        test_api_client, _ = test_api_client_persistent
        (
            response_dict,
            feature_id,
        ) = self._create_fl_namespace_with_manual_version_mode_and_new_feature_id(test_api_client)

        # update feature list namespace status to TEMPLATE
        doc_id = response_dict["_id"]
        response = test_api_client.patch(f"{self.base_route}/{doc_id}", json={"status": "TEMPLATE"})
        response_dict = response.json()
        assert response_dict["default_version_mode"] == "MANUAL"
        assert response_dict["status"] == "TEMPLATE"
        assert response_dict["readiness_distribution"] == [{"count": 1, "readiness": "DRAFT"}]

        # upgrade default feature_sum_30m to production ready & check the default feature list
        # readiness distribution get updated
        response = test_api_client.patch(
            f"feature/{feature_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response_dict["readiness_distribution"] == [
            {"count": 1, "readiness": "PRODUCTION_READY"}
        ]

    def test_update__set_default_feature_list_id_on_manual_mode(self, test_api_client_persistent):
        """Test update: set default feature list id on manual mode"""
        test_api_client, _ = test_api_client_persistent
        (
            response_dict,
            feature_id,
        ) = self._create_fl_namespace_with_manual_version_mode_and_new_feature_id(test_api_client)
        feature_list_namespace_id = response_dict["_id"]

        # create a new feature version with production ready readiness
        feature_list_id = response_dict["feature_list_ids"][0]
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
        test_api_client.patch(
            f"feature/{new_feature_id}",
            json={"readiness": "PRODUCTION_READY", "ignore_guardrails": True},
        )

        # create a new feature list version
        post_feature_list_response = test_api_client.post(
            "/feature_list", json={"source_feature_list_id": feature_list_id, "features": []}
        )
        new_feature_list_id = post_feature_list_response.json()["_id"]

        # check feature list namespace response (make sure new feature list ID is included)
        namespace_response = test_api_client.get(f"{self.base_route}/{feature_list_namespace_id}")
        namespace_response_dict = namespace_response.json()
        assert namespace_response_dict["default_version_mode"] == "MANUAL"
        assert namespace_response_dict["feature_list_ids"] == [feature_list_id, new_feature_list_id]
        assert namespace_response_dict["default_feature_list_id"] == feature_list_id
        assert namespace_response_dict["readiness_distribution"] == [
            {"count": 1, "readiness": "DRAFT"}
        ]

        # test update new feature list ID
        update_response = test_api_client.patch(
            f"{self.base_route}/{feature_list_namespace_id}",
            json={"default_feature_list_id": new_feature_list_id},
        )
        update_response_dict = update_response.json()
        assert update_response.status_code == HTTPStatus.OK
        assert update_response_dict["feature_list_ids"] == [feature_list_id, new_feature_list_id]
        assert update_response_dict["default_feature_list_id"] == new_feature_list_id
        assert update_response_dict["readiness_distribution"] == [
            {"count": 1, "readiness": "PRODUCTION_READY"}
        ]

        # check update default version mode to AUTO
        update_response = test_api_client.patch(
            f"{self.base_route}/{feature_list_namespace_id}", json={"default_version_mode": "AUTO"}
        )
        update_response_dict = update_response.json()
        assert update_response.status_code == HTTPStatus.OK
        assert update_response_dict["default_version_mode"] == "AUTO"
        assert update_response_dict["default_feature_list_id"] == new_feature_list_id

        # check update default feature list ID on AUTO mode
        update_response = test_api_client.patch(
            f"{self.base_route}/{feature_list_namespace_id}",
            json={"default_feature_list_id": feature_list_id},
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert update_response.json()["detail"] == (
            "Cannot set default feature list ID when default version mode is not MANUAL."
        )

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
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}
            ],
            "primary_entity": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "default"}
            ],
            "feature_namespace_ids": create_response_dict["feature_namespace_ids"],
            "default_feature_ids": response_dict["default_feature_ids"],
            "default_version_mode": "AUTO",
            "default_feature_list_id": response_dict["default_feature_list_id"],
            "dtype_distribution": [{"count": 2, "dtype": "FLOAT"}],
            "version_count": 1,
            "feature_count": 2,
            "status": "DRAFT",
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
        """Post route success response object"""
        api_client, _ = test_api_client_persistent
        self.setup_creation_route(api_client, catalog_id=catalog_id)

        feature_list_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_list_multi.json"
        )
        feature_list_response = api_client.post(
            "/feature_list",
            headers={"active-catalog-id": str(catalog_id)},
            json=feature_list_payload,
        )
        feature_list_namespace_id = feature_list_response.json()["feature_list_namespace_id"]

        response = api_client.get(
            f"{self.base_route}/{feature_list_namespace_id}",
            headers={"active-catalog-id": str(catalog_id)},
        )
        assert response.status_code == HTTPStatus.OK
        return response
