"""
Test for target routes
"""

from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestTargetApi(BaseCatalogApiTestSuite):
    """
    TestTargetApi class
    """

    class_name = "Target"
    base_route = "/target"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/target.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Target (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `Target.get(name="target")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'Target (name: "target") already exists. '
            'Get the existing object by `Target.get(name="target")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "description": ["cust_id"]},
            [
                {
                    "loc": ["body", "description"],
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
            f'Target (id: "{unknown_id}") not found. Please save the Target object first.',
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
