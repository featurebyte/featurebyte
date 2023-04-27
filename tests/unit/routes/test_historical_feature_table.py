"""
Tests for HistoricalFeatureTable routes
"""
import copy
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId
from sqlglot import expressions

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseAsyncApiTestSuite


class TestHistoricalFeatureTableApi(BaseAsyncApiTestSuite):
    """
    Tests for HistoricalFeatureTable route
    """

    class_name = "HistoricalFeatureTable"
    base_route = "/historical_feature_table"
    payload = BaseAsyncApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/historical_feature_table.json"
    )
    random_id = str(ObjectId())

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'HistoricalFeatureTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `HistoricalFeatureTable.get(name="{payload["name"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'HistoricalFeatureTable (name: "{payload["name"]}") already exists. '
            f'Get the existing object by `HistoricalFeatureTable.get(name="{payload["name"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "random_name",
                "observation_table_id": random_id,
            },
            f'ObservationTable (id: "{random_id}") not found. Please save the ObservationTable object first.',
        ),
    ]

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("context", "context"),
            ("observation_table", "observation_table"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
            ("deployment", "deployment"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}",
                headers={"active-catalog-id": str(catalog_id)},
                json=payload,
            )
            if api_object == "feature":
                self.make_feature_production_ready(api_client, response.json()["_id"], catalog_id)

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
        Patch ObservationTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_observation_table_service

    @pytest.fixture(autouse=True)
    def always_patched_get_historical_feature(self):
        """
        Patch parts of compute_historical_features that have coverage elsewhere and not relevant to unit
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

    def test_create_422__failed_entity_validation_check(self, test_api_client_persistent):
        """Test that 422 is returned when payload fails validation check"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = copy.deepcopy(self.payload)
        payload["featurelist_get_historical_features"]["serving_names_mapping"] = {
            "random_name": "random_name"
        }

        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Unexpected serving names provided in serving_names_mapping: random_name"
        )

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert isinstance(response_dict["feature_list_version"], str)
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": self.payload["name"],
            "feature_list_name": "sf_feature_list",
            "feature_list_version": response_dict["feature_list_version"],
            "observation_table_name": "observation_table",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
        }
