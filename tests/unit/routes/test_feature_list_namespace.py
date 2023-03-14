"""
Test for FeatureListNamespace route
"""
import time
from http import HTTPStatus
from unittest.mock import Mock

import pytest
import pytest_asyncio
from bson import ObjectId
from requests import Response

from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.models.feature_list import FeatureListNamespaceModel
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from tests.unit.routes.base import BaseWorkspaceApiTestSuite


class TestFeatureListNamespaceApi(BaseWorkspaceApiTestSuite):
    """
    TestFeatureListNamespaceApi
    """

    class_name = "FeatureListNamespace"
    base_route = "/feature_list_namespace"
    payload = BaseWorkspaceApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_namespace.json"
    )
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

    @property
    def class_name_to_save(self):
        """Class name used to save the object"""
        return "FeatureList"

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
        feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
        )
        document = await feature_list_namespace_service.create_document(
            data=FeatureListNamespaceModel(**self.payload)
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
    def test_create_201_non_default_workspace(
        self,
        workspace_id,
        create_success_response_non_default_workspace,
    ):
        """Test creation (success) in non default workspace"""

    async def setup_get_info(self, api_client):
        """Setup for get_info route testing"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
            ("feature", "feature_sum_2h"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    @pytest_asyncio.fixture
    async def create_multiple_success_responses(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post multiple success responses"""
        test_api_client, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
        )
        output = []
        for _, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
            document = await feature_list_namespace_service.create_document(
                data=FeatureListNamespaceModel(**payload)
            )
            output.append(document)
            time.sleep(0.05)
        return output

    def _create_fl_namespace_with_manual_version_mode_and_new_feature_id(self, test_api_client):
        """Create feature list namespace with manual version mode"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
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
        doc_id = payload["feature_list_namespace_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"default_version_mode": "MANUAL"}
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

        # update feature list namespace status to PUBLISHED
        doc_id = response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"status": "PUBLISHED"}
        )
        response_dict = response.json()
        assert response_dict["default_version_mode"] == "MANUAL"
        assert response_dict["status"] == "PUBLISHED"
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
        test_api_client.patch(
            f"feature/{new_feature_id}",
            json={"readiness": "PRODUCTION_READY", "ignore_guardrails": True},
        )

        # create a new feature list version
        post_feature_list_response = test_api_client.post(
            "/feature_list", json={"source_feature_list_id": feature_list_id, "mode": "auto"}
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
            "Cannot set default feature list ID when default version mode is not MANUAL"
        )

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        await self.setup_get_info(test_api_client)
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict == {
            "name": "sf_feature_list_multiple",
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "workspace_name": "default"}
            ],
            "tabular_data": [
                {"name": "sf_event_data", "status": "DRAFT", "workspace_name": "default"}
            ],
            "default_version_mode": "AUTO",
            "default_feature_list_id": response_dict["default_feature_list_id"],
            "dtype_distribution": [{"count": 2, "dtype": "FLOAT"}],
            "version_count": 1,
            "feature_count": 2,
            "status": "DRAFT",
            "workspace_name": "default",
        }

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert verbose_response.status_code == HTTPStatus.OK, verbose_response.text

    @pytest_asyncio.fixture
    async def create_success_response_non_default_workspace(
        self, test_api_client_persistent, user_id, workspace_id
    ):  # pylint: disable=arguments-differ
        """Post route success response object"""
        _, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent, workspace_id=workspace_id
        )
        document = await feature_list_namespace_service.create_document(
            data=FeatureListNamespaceModel(**self.payload)
        )
        response = Response()
        response._content = bytes(document.json(by_alias=True), "utf-8")
        response.status_code = HTTPStatus.CREATED
        return response
