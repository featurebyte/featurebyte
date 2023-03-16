"""
Test relationship info routes
"""
from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.schema.relationship_info import RelationshipInfoUpdate
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestRelationshipInfoApi(BaseCatalogApiTestSuite):
    """
    Test relationship info routes
    """

    class_name = "RelationshipInfo"
    base_route = "/relationship_info"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/relationship_info.json"
    )
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'RelationshipInfo (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `RelationshipInfo.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                "relationship_type": "child_parent",
                "is_enabled": False,
            },
            [
                {"loc": ["body", "name"], "msg": "field required", "type": "value_error.missing"},
                {
                    "loc": ["body", "primary_entity_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "related_entity_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "primary_data_source_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        ),
    ]

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        # Create 3 distinct pairs of entity IDs so that the payload generated here is unique.
        entity_ids = [
            "63f94ed6ea1f050131379214",
            "63f94ed6ea1f050131379204",
            "63f94ed6ea1f050131379204",
            "63f6a145e549df8ccf2bf3f2",
            "63f6a145e549df8ccf2bf3f2",
            "63f6a145e549df8ccf2bf3f3",
        ]
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["primary_entity_id"] = entity_ids[i * 2]
            payload["related_entity_id"] = entity_ids[i * 2 + 1]
            payload["primary_data_source_id"] = "6337f9651050ee7d5980660d"
            payload["updated_by"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("event_table", "event_table"),
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("entity", "entity_user"),
            ("entity", "entity_country"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", params={"catalog_id": catalog_id}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent

        # Create relationship info
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # Get info
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict["relationship_type"] == "child_parent"
        assert response_dict["primary_entity_name"] == "customer"
        assert response_dict["related_entity_name"] == "transaction"
        assert response_dict["data_source_name"] == "sf_event_data"

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """
        Test patch to update is_enabled status
        """
        # Create RelationshipInfo and verify is_enabled is True
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        assert response_dict["is_enabled"]

        # Update is_enabled to False
        data_update = RelationshipInfoUpdate(is_enabled=False)
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}", json=data_update.dict()
        )
        assert response.status_code == HTTPStatus.OK

        # Verify is_enabled is False
        response = test_api_client.get(f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert not response_dict["is_enabled"]
