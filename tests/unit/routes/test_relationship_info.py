"""
Test relationship info routes
"""
from bson import ObjectId

from tests.unit.routes.base import BaseWorkspaceApiTestSuite


class TestRelationshipInfoApi(BaseWorkspaceApiTestSuite):
    """
    Test relationship info routes
    """

    class_name = "RelationshipInfo"
    base_route = "/relationship_info"
    payload = BaseWorkspaceApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/relationship_info.json"
    )

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["child_id"] = str(ObjectId())
            payload["parent_id"] = str(ObjectId())
            payload["child_data_source_id"] = str(ObjectId())
            payload["updated_by"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload
