"""
Test relationship info routes
"""
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
