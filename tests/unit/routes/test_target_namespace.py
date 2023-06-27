"""
Test for target namespace routes
"""

import pytest
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
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'TargetNamespace (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `TargetNamespace.get(name="target_namespace")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'TargetNamespace (name: "target_namespace") already exists. '
            'Get the existing object by `TargetNamespace.get(name="target_namespace")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "node_name": ["cust_id"]},
            [
                {
                    "loc": ["body", "node_name"],
                    "msg": "str type expected",
                    "type": "type_error.str",
                }
            ],
        )
    ]
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

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

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

    @pytest.mark.skip("implement with update of target endpoints")
    def test_create_409(
        self,
        test_api_client_persistent,
        create_success_response,
        create_conflict_payload_expected_detail,
    ):
        """Test creation (conflict)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_create_422(
        self,
        test_api_client_persistent,
        create_success_response,
        create_unprocessable_payload_expected_detail,
    ):
        """Test creation (unprocessable entity)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_get_200(self, test_api_client_persistent, create_success_response, user_id):
        """Test get (success)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_list_200(self, test_api_client_persistent, create_multiple_success_responses):
        """Test list (success, multiple)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_list_501(self, test_api_client_persistent, create_success_response):
        """Test list (not implemented)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_list_audit_422(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
        list_unprocessable_params_expected_detail,
    ):
        """Test list audit (unprocessable)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_get_200_non_default_catalog(
        self,
        test_api_client_persistent,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test get (success)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_list_200_non_default_catalog(
        self,
        test_api_client_persistent,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test get (success)"""

    @pytest.mark.skip("implement with update of target endpoints")
    def test_list_422(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
        list_unprocessable_params_expected_detail,
    ):
        """Test list (unprocessable)"""
