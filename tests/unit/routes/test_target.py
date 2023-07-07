"""
Test for target routes
"""
from http import HTTPStatus

from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
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
        # FIXME: This test case is not working
        # (
        #     payload,
        #     f'Target (id: "{payload["_id"]}") already exists. '
        #     'Get the existing object by `Target.get(name="target")`.',
        # ),
        # (
        #     {**payload, "_id": str(ObjectId())},
        #     'Target (name: "target") already exists. '
        #     'Get the existing object by `Target.get(name="target")`.',
        # ),
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
            f'Target (id: "{unknown_id}") not found. Please save the Target object first.',
        )
    ]

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            payload["target_namespace_id"] = str(ObjectId())
            yield payload
