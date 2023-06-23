"""
Test for UserDefinedFunction route
"""
from http import HTTPStatus

from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseApiTestSuite


class TestUserDefinedFunctionApi(BaseApiTestSuite):
    """
    Test for UserDefinedFunction route
    """

    class_name = "UserDefinedFunction"
    base_route = "/user_defined_function"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/user_defined_function.json"
    )

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """Setup for creation route"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            payload["_id"] = self.payload["feature_store_id"]
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """
        Multiple success payload generator
        """
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{payload["name"]}_{i}'
            yield payload
