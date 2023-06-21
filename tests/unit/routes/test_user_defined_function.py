"""
Test for UserDefinedFunction route
"""
from bson import ObjectId

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
