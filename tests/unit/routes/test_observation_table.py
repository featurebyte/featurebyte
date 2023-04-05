"""
Tests for ObservationTable routes
"""
from http import HTTPStatus

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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        # save feature store
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        response = api_client.post(
            "/feature_store", params={"catalog_id": catalog_id}, json=payload
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
