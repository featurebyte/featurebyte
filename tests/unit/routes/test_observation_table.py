"""
Tests for ObservationTable routes
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseAsyncApiTestSuite


class TestObservationTableApi(BaseAsyncApiTestSuite):
    """
    Tests for ObservationTable route
    """

    class_name = "ObservationTable"
    base_route = "/observation_table"
    payload = BaseAsyncApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/observation_table.json"
    )

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'ObservationTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `ObservationTable.get(name="{payload["name"]}")`.',
        ),
    ]

    unknown_context_id = str(ObjectId())
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "new_table",
                "context_id": unknown_context_id,
            },
            f'Context (id: "{unknown_context_id}") not found. Please save the Context object first.',
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
    def always_patched_observation_table_service(self, patched_observation_table_service):
        """
        Patch get_most_recent_point_in_time_sql
        """
        _ = patched_observation_table_service
