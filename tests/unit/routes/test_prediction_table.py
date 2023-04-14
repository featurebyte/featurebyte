"""
Tests for PredictionTable routes
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseAsyncApiTestSuite


class TestPredictionTableApi(BaseAsyncApiTestSuite):
    """
    Tests for PredictionTable route
    """

    class_name = "PredictionTable"
    base_route = "/prediction_table"
    payload = BaseAsyncApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/prediction_table.json"
    )

    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store.json"),
            ("entity", "entity.json"),
            ("context", "context.json"),
            ("observation_table", "observation_table.json"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}")
            response = api_client.post(
                f"/{api_object}",
                headers={"active-catalog-id": str(catalog_id)},
                json=payload,
            )
            if api_object == "observation_table":
                response = self.wait_for_results(api_client, response)
                assert response.json()["status"] == "SUCCESS"
            else:
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
        Patch ObservationTableService so get_additional_metadata always passes
        """
        _ = patched_observation_table_service
