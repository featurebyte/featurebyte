"""
Tests for StaticSourceTable routes
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseMaterializedTableTestSuite


class TestStaticSourceTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for StaticSourceTable route
    """

    class_name = "StaticSourceTable"
    base_route = "/static_source_table"
    payload = BaseMaterializedTableTestSuite.load_payload(
        "tests/fixtures/request_payloads/static_source_table.json"
    )
    async_create = True

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'StaticSourceTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `StaticSourceTable.get(name="{payload["name"]}")`.',
        ),
    ]

    unknown_feature_store_id = str(ObjectId())
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "new_table",
                "feature_store_id": unknown_feature_store_id,
            },
            f'FeatureStore (id: "{unknown_feature_store_id}") not found. Please save the FeatureStore object first.',
        )
    ]

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        # save feature store
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        response = api_client.post(
            "/feature_store", headers={"active-catalog-id": str(catalog_id)}, json=payload
        )
        assert response.status_code == HTTPStatus.CREATED

        # save entity
        payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        response = api_client.post(
            "/entity", headers={"active-catalog-id": str(catalog_id)}, json=payload
        )
        assert response.status_code == HTTPStatus.CREATED

        # save context
        payload = self.load_payload("tests/fixtures/request_payloads/context.json")
        response = api_client.post(
            "/context", headers={"active-catalog-id": str(catalog_id)}, json=payload
        )
        assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    @pytest.fixture(autouse=True)
    def always_patched_static_source_table_service(self, patched_static_source_table_service):
        """
        Patch StaticSourceTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_static_source_table_service

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": self.payload["name"],
            "type": "source_table",
            "feature_store_name": "sf_featurestore",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
        }
