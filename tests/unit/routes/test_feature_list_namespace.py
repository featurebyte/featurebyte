"""
Test for FeatureListNamespace route
"""
import asyncio
import time
from http import HTTPStatus
from unittest.mock import Mock

import pytest
from bson import ObjectId
from requests import Response

from featurebyte.schema.feature_list_namespace import FeatureListNamespaceCreate
from featurebyte.service.feature_list_namespace import FeatureListNamespaceService
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureListNamespaceApi(BaseApiTestSuite):
    """
    TestFeatureListNamespaceApi
    """

    class_name = "FeatureListNamespace"
    base_route = "/feature_list_namespace"
    payload = BaseApiTestSuite.load_payload(
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
        for _ in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            yield payload

    @pytest.fixture
    def create_success_response(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post route success response object"""
        _, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent
        )
        document = asyncio.run(
            feature_list_namespace_service.create_document(
                data=FeatureListNamespaceCreate(**self.payload)
            )
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

    @pytest.fixture
    def create_multiple_success_responses(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post multiple success responses"""
        test_api_client, persistent = test_api_client_persistent
        user = Mock()
        user.id = user_id
        feature_list_namespace_service = FeatureListNamespaceService(
            user=user, persistent=persistent
        )
        output = []
        for i, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
            payload["name"] = f'{self.payload["name"]}_{i}'
            document = asyncio.run(
                feature_list_namespace_service.create_document(
                    data=FeatureListNamespaceCreate(**payload)
                )
            )
            output.append(document)
            time.sleep(0.05)
        return output
