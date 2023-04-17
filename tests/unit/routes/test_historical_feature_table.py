"""
Tests for ModelingTable routes
"""
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId
from sqlglot import expressions

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseAsyncApiTestSuite


class TestModelingTableApi(BaseAsyncApiTestSuite):
    """
    Tests for ModelingTable route
    """

    class_name = "ModelingTable"
    base_route = "/modeling_table"
    payload = BaseAsyncApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/modeling_table.json"
    )

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'ModelingTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `ModelingTable.get(name="{payload["name"]}")`.',
        ),
    ]

    create_unprocessable_payload_expected_detail_pairs = []

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

        # save observation table
        payload = self.load_payload("tests/fixtures/request_payloads/observation_table.json")
        response = api_client.post(
            "/observation_table", headers={"active-catalog-id": str(catalog_id)}, json=payload
        )
        response = self.wait_for_results(api_client, response)
        assert response.json()["status"] == "SUCCESS"

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
        Patch ObservationTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_observation_table_service

    @pytest.fixture(autouse=True)
    def always_patched_get_historical_feature(self):
        """
        Patch parts of get_historical_features that have coverage elsewhere and not relevant to unit
        testing the routes
        """
        with patch(
            "featurebyte.query_graph.sql.feature_historical.get_historical_features_expr",
            return_value=expressions.select("*").from_("my_table"),
        ):
            with patch(
                "featurebyte.query_graph.sql.feature_historical.compute_tiles_on_demand",
            ):
                yield
