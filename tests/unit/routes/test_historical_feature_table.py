"""
Tests for HistoricalFeatureTable routes
"""

import copy
import textwrap
from http import HTTPStatus
from unittest.mock import patch

import pandas as pd
import pytest
from bson.objectid import ObjectId
from pandas.testing import assert_frame_equal
from sqlglot import expressions

from featurebyte.common.utils import dataframe_from_json, dataframe_to_arrow_bytes
from featurebyte.query_graph.sql.feature_compute import FeatureQueryPlan
from tests.unit.routes.base import BaseMaterializedTableTestSuite


class TestHistoricalFeatureTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for HistoricalFeatureTable route
    """

    wrap_payload_on_create = True

    class_name = "HistoricalFeatureTable"
    base_route = "/historical_feature_table"
    payload = BaseMaterializedTableTestSuite.load_payload(
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

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("context", "context"),
            ("observation_table", "observation_table"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
            ("deployment", "deployment"),
        ]
        catalog_id = api_client.get("/catalog").json()["data"][0]["_id"]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
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
    def always_patched_get_historical_feature(self, mocked_compute_tiles_on_demand):
        """
        Patch parts of compute_historical_features that have coverage elsewhere and not relevant to unit
        testing the routes
        """
        _ = mocked_compute_tiles_on_demand
        with patch(
            "featurebyte.query_graph.sql.feature_historical.get_historical_features_expr",
            return_value=FeatureQueryPlan(
                common_tables=[],
                post_aggregation_sql=expressions.select("*").from_("my_table"),
                feature_names=["sum_30m"],
            ),
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

        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Unexpected serving names provided in serving_names_mapping: random_name"
        )

    @pytest.mark.asyncio
    async def test_observation_table_delete_422__observation_table_failed_validation_check(
        self, test_api_client_persistent, create_success_response, user_id, default_catalog_id
    ):
        """Test delete 422 for observation table failed validation check"""
        test_api_client, persistent = test_api_client_persistent
        create_success_response_dict = create_success_response.json()

        # insert another document to historical feature table to make sure the query filter is correct
        payload = copy.deepcopy(self.payload)
        payload["_id"] = ObjectId()
        payload["name"] = "random_name"
        await persistent.insert_one(
            collection_name="historical_feature_table",
            document={
                **payload,
                "_id": ObjectId(),
                "catalog_id": ObjectId(default_catalog_id),
                "user_id": user_id,
                "observation_table_id": ObjectId(),  # different batch request table id
                "columns_info": [],
                "num_rows": 500,
                "location": create_success_response_dict["location"],
                "feature_list_id": ObjectId(),
            },
            user_id=user_id,
        )
        response = test_api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["total"] == 2

        # try to delete observation table
        observation_table_id = self.payload["observation_table_id"]
        response = test_api_client.delete(f"/observation_table/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            "ObservationTable is referenced by HistoricalFeatureTable: historical_feature_table"
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
            "description": None,
        }

    def test_provide_both_observation_table_id_and_dataframe_not_allowed(
        self, test_api_client_persistent
    ):
        """
        Test that providing both observation_table_id and observation set DataFrame is not allowed
        """
        test_api_client, _ = test_api_client_persistent
        df = pd.DataFrame({
            "POINT_IN_TIME": ["2023-01-15 10:00:00"],
            "CUST_ID": ["C1"],
        })
        files = {"observation_set": dataframe_to_arrow_bytes(df)}
        response = self.post(test_api_client, self.payload, files=files)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": "Only one of observation_set file and observation_table_id can be set"
        }

    def test_create__success_feature_clusters_not_set(self, test_api_client_persistent):
        """Test created successfully when feature_clusters is not set"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = copy.deepcopy(self.payload)
        payload["featurelist_get_historical_features"]["feature_clusters"] = None

        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        assert response.json()["output_path"] is not None
        assert response.json()["status"] == "SUCCESS"

    def test_create__failed_feature_clusters_and_feature_list_id_not_set(
        self, test_api_client_persistent
    ):
        """Test created failed when neither feature_clusters nor feature_list_id is set"""
        test_api_client, _ = test_api_client_persistent

        # verify that feature clusters or feature list id must be set
        payload = copy.deepcopy(self.payload)
        payload["featurelist_get_historical_features"]["feature_clusters"] = None
        payload["featurelist_get_historical_features"]["feature_list_id"] = None
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert "Either feature_clusters or feature_list_id must be set" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_update_name(self, test_api_client_persistent, create_success_response):
        """Test update name"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"name": "some other name"}
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["name"] == "some other name"

    def test_delete_feature_list_of_historical_feature_table(
        self, create_success_response, test_api_client_persistent
    ):
        """Test delete feature list of historical feature table"""
        feature_list_id = create_success_response.json()["feature_list_id"]

        test_api_client, _ = test_api_client_persistent
        response = test_api_client.delete(f"/feature_list/{feature_list_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "FeatureList is referenced by Deployment: my_deployment"

    def test_preview_historical_feature_table(
        self, create_success_response, test_api_client_persistent, mock_get_session
    ):
        """Test preview features in historical feature table"""
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]
        table_name = response_dict["location"]["table_details"]["table_name"]

        expected_df = pd.DataFrame({
            "POINT_IN_TIME": ["2021-01-01 00:00:00", "2021-01-01 00:00:00"],
            "cust_id": [1, 2],
            "sum_30m": [1.0, 2.0],
        })
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df

        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/feature_preview/646f6c1b0ed28a5271fb02c4"
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        result_df = dataframe_from_json(response.json())
        assert_frame_equal(result_df, expected_df)

        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                f"""
            SELECT
              "POINT_IN_TIME",
              "cust_id",
              "sum_30m"
            FROM "sf_database"."sf_schema"."{table_name}"
            ORDER BY
              "__FB_TABLE_ROW_INDEX"
            LIMIT 10
            """
            ).strip()
        )
