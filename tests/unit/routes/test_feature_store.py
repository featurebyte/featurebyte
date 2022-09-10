"""
Test for FeatureStore route
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureStoreApi(BaseApiTestSuite):
    """
    TestFeatureStoreApi
    """

    class_name = "FeatureStore"
    base_route = "/feature_store"
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/feature_store.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'FeatureStore (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `FeatureStore.get(name="sf_featurestore")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'FeatureStore (name: "sf_featurestore") already exists. '
            'Get the existing object by `FeatureStore.get(name="sf_featurestore")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {key: val for key, val in payload.items() if key != "name"},
            [
                {
                    "loc": ["body", "name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
        )
    ]

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["details"] = {
                key: f"{value}_{i}" for key, value in self.payload["details"].items()
            }
            yield payload

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert (
            response_dict.items()
            > {
                "name": "sf_featurestore",
                "update_date": None,
                "source": "snowflake",
                "database_details": {
                    "account": "sf_account",
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
            }.items()
        )
        assert "creation_date" in response_dict
