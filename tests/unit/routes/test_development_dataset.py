"""
Test for DevelopmentDataset route
"""

import copy
import json
from http import HTTPStatus
from unittest import mock

import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.session.base import DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestDevelopmentDatasetApi(BaseCatalogApiTestSuite):
    """
    Test for DevelopmentDataset route
    """

    class_name = "DevelopmentDataset"
    base_route = "/development_dataset"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/development_dataset.json"
    )

    def setup_creation_route(self, api_client):
        """Setup for post route"""
        api_object_filename_pairs = [
            ("event_table", "event_table"),
            ("dimension_table", "dimension_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """
        Multiple success payload generator
        """
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{payload["name"]}_{i}'
            yield payload

    @pytest.fixture(autouse=True)
    def snowflake_execute_query_for_dev_dataset_fixture(
        self,
        snowflake_connector,
        snowflake_query_map,
    ):
        """
        Extended version of the default execute_query mock to handle more queries expected for the tests
        """
        _ = snowflake_connector

        def side_effect(
            query,
            timeout=DEFAULT_EXECUTE_QUERY_TIMEOUT_SECONDS,
            to_log_error=True,
            query_metadata=None,
        ):
            _ = timeout, to_log_error, query_metadata
            if (
                query.startswith('SHOW COLUMNS IN "sf_database"."sf_schema"."sf_table_sample"')
                or query.startswith(
                    'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_dim_table_sample"'
                )
                or query.startswith(
                    'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_mismatch_table_sample"'
                )
            ):
                res = [
                    {
                        "column_name": "col_int",
                        "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                        "comment": None,
                    },
                    {
                        "column_name": "col_float",
                        "data_type": json.dumps({"type": "REAL"}),
                        "comment": "Float column",
                    },
                    {
                        "column_name": "col_char",
                        "data_type": json.dumps({"type": "TEXT", "length": 1}),
                        "comment": "Char column",
                    },
                    {
                        "column_name": "col_text",
                        "data_type": json.dumps({"type": "TEXT", "length": 2**24}),
                        "comment": "Text column",
                    },
                    {
                        "column_name": "col_binary",
                        "data_type": json.dumps({"type": "BINARY"}),
                        "comment": None,
                    },
                    {
                        "column_name": "col_boolean",
                        "data_type": json.dumps({"type": "BOOLEAN"}),
                        "comment": None,
                    },
                    {
                        "column_name": "event_timestamp",
                        "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                        "comment": "Timestamp column",
                    },
                    {
                        "column_name": "created_at",
                        "data_type": json.dumps({"type": "TIMESTAMP_TZ"}),
                        "comment": None,
                    },
                    {
                        "column_name": "cust_id",
                        "data_type": json.dumps({"type": "FIXED", "scale": 0}),
                        "comment": None,
                    },
                ]
            else:
                res = snowflake_query_map.get(query)
            if res is not None:
                if query.startswith(
                    'SHOW COLUMNS IN "sf_database"."sf_schema"."sf_mismatch_table_sample"'
                ):
                    res[-1]["data_type"] = json.dumps({"type": "TIMESTAMP_TZ"})
                return pd.DataFrame(res)
            return None

        with mock.patch(
            "featurebyte.session.snowflake.SnowflakeSession.execute_query"
        ) as mock_execute_query:
            mock_execute_query.side_effect = side_effect
            yield mock_execute_query

    def test_create__non_existent_table(self, test_api_client_persistent):
        """Test create route (non-existent table)"""
        test_api_client, _ = test_api_client_persistent
        payload = self.payload.copy()
        payload["development_tables"] = copy.deepcopy(payload["development_tables"])
        payload["development_tables"][0]["table_id"] = str(ObjectId("686bd2de38662d2da67a08ec"))
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == (
            'Table (id: "686bd2de38662d2da67a08ec") not found. Please save the Table object first.'
        )

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """Test update development dataset(success)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]

        # check update function parameter
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}",
            json={
                "name": "new name",
                "development_tables": [
                    {
                        "table_id": "6337f9651050ee7d1234660d",
                        "location": {
                            "feature_store_id": "646f6c190ed28a5271fb02a1",
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_dim_table_sample",
                            },
                        },
                    },
                ],
            },
        )
        assert update_response.status_code == HTTPStatus.OK
        update_response_dict = update_response.json()
        expected_response_dict = response_dict.copy()
        expected_response_dict["updated_at"] = update_response_dict["updated_at"]
        expected_response_dict["name"] = "new name"
        expected_response_dict["development_tables"] = [
            {
                "table_id": "6337f9651050ee7d5980660d",
                "location": {
                    "feature_store_id": "646f6c190ed28a5271fb02a1",
                    "table_details": {
                        "database_name": "sf_database",
                        "schema_name": "sf_schema",
                        "table_name": "sf_table_sample",
                    },
                },
            },
            {
                "table_id": "6337f9651050ee7d1234660d",
                "location": {
                    "feature_store_id": "646f6c190ed28a5271fb02a1",
                    "table_details": {
                        "database_name": "sf_database",
                        "schema_name": "sf_schema",
                        "table_name": "sf_dim_table_sample",
                    },
                },
            },
        ]
        assert update_response_dict == expected_response_dict

    def test_update_404(self, test_api_client_persistent):
        """Test update development dataset(not found)"""
        test_api_client, _ = test_api_client_persistent

        random_id = ObjectId()
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{random_id}", json={"name": "new name"}
        )
        assert update_response.status_code == HTTPStatus.NOT_FOUND

    def test_update_422__duplicate_table(self, test_api_client_persistent, create_success_response):
        """Test update development dataset(duplicate table)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]

        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}",
            json={
                "development_tables": [
                    {
                        "table_id": "6337f9651050ee7d5980660d",
                        "location": {
                            "feature_store_id": "646f6c190ed28a5271fb02a1",
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_dim_table_sample",
                            },
                        },
                    },
                ]
            },
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert "Duplicate table IDs found in development tables" in update_response.json()["detail"]

    def test_update_422__missing_columns(self, test_api_client_persistent, create_success_response):
        """Test update development dataset(missing columns)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]

        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}",
            json={
                "development_tables": [
                    {
                        "table_id": "6337f9651050ee7d5980660d",
                        "location": {
                            "feature_store_id": "646f6c190ed28a5271fb02a1",
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_other_table_sample",
                            },
                        },
                    },
                ]
            },
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            'Development source for table "sf_event_table" missing required columns: col_binary, col_boolean,'
            in update_response.json()["detail"]
        )

    def test_update_422__mismatch_column_dtype(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update development dataset(mismatched column dtype)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]

        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}",
            json={
                "development_tables": [
                    {
                        "table_id": "6337f9651050ee7d5980660d",
                        "location": {
                            "feature_store_id": "646f6c190ed28a5271fb02a1",
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_mismatch_table_sample",
                            },
                        },
                    },
                ]
            },
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            'Development source for table "sf_event_table" column type mismatch: cust_id (expected INT, got TIMESTAMP_TZ)'
            in update_response.json()["detail"]
        )

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete development dataset(success)"""
        test_api_client, _ = test_api_client_persistent

        # test delete development dataset
        response = test_api_client.delete(
            url=f"{self.base_route}/{create_success_response.json()['_id']}"
        )
        assert response.status_code == HTTPStatus.OK

        # check the development datasetis deleted
        response = test_api_client.get(f"{self.base_route}/{create_success_response.json()['_id']}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_404(self, test_api_client_persistent):
        """Test delete development dataset(not found)"""
        test_api_client, _ = test_api_client_persistent

        random_id = ObjectId()
        response = test_api_client.delete(url=f"{self.base_route}/{random_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test get info route (200)"""
        test_api_client, _ = test_api_client_persistent
        model_response_dict = create_success_response.json()
        # test get info
        response = test_api_client.get(
            url=f"{self.base_route}/{create_success_response.json()['_id']}/info"
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        response_dict = response.json()
        assert response_dict == {
            "name": "My Development Dataset",
            "created_at": model_response_dict["created_at"],
            "description": "This is a development dataset",
            "updated_at": None,
            "development_tables": [
                {
                    "feature_store_name": "sf_featurestore",
                    "table_details": {
                        "database_name": "sf_database",
                        "schema_name": "sf_schema",
                        "table_name": "sf_table_sample",
                    },
                    "table_name": "sf_event_table",
                }
            ],
            "sample_from_timestamp": "2022-01-01T00:00:00",
            "sample_to_timestamp": "2024-12-31T00:00:00",
        }

    def test_get_schema(self, test_api_client_persistent, create_success_response):
        """Test get route response schema"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()

        response = test_api_client.get(url=f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response.json() == {
            "_id": response_dict["_id"],
            "user_id": "63f9506dd478b94127123456",
            "name": "My Development Dataset",
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "block_modification_by": [],
            "description": "This is a development dataset",
            "is_deleted": False,
            "catalog_id": "646f6c1c0ed28a5271fb02db",
            "sample_from_timestamp": "2022-01-01T00:00:00",
            "sample_to_timestamp": "2024-12-31T00:00:00",
            "development_tables": [
                {
                    "location": {
                        "feature_store_id": "646f6c190ed28a5271fb02a1",
                        "table_details": {
                            "database_name": "sf_database",
                            "schema_name": "sf_schema",
                            "table_name": "sf_table_sample",
                        },
                    },
                    "table_id": "6337f9651050ee7d5980660d",
                }
            ],
        }
