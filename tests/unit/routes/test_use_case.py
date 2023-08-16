"""
Tests for Use Case route
"""
from http import HTTPStatus

from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestUseCaseApi(BaseCatalogApiTestSuite):
    """
    TestUseCaseApi class
    """

    class_name = "UseCase"
    base_route = "/use_case"
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/use_case.json")
    unknown_id = ObjectId()
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'UseCase (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `UseCase.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "context_id": "test_id"},
            [
                {
                    "loc": ["body", "context_id"],
                    "msg": "Id must be of type PydanticObjectId",
                    "type": "type_error",
                }
            ],
        )
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
            ("context", "context"),
            ("target", "target"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload
