"""
Test for FeatureStore route
"""

import copy
import json
import textwrap
from datetime import datetime
from http import HTTPStatus
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId
from pandas.testing import assert_frame_equal
from snowflake.connector.errors import ProgrammingError

from featurebyte import FeatureStore
from featurebyte.common.utils import dataframe_from_json, dataframe_to_json
from featurebyte.exception import CredentialsError
from featurebyte.models.credential import (
    CredentialModel,
    S3StorageCredential,
    UsernamePasswordCredential,
)
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.query_graph.model.table import TableSpec
from featurebyte.query_graph.node.schema import TableDetails
from featurebyte.schema.feature_store import (
    ComputeOption,
    FeatureStorePreview,
    FeatureStoreQueryPreview,
    FeatureStoreSample,
)
from tests.unit.routes.base import BaseApiTestSuite
from tests.util.helper import assert_equal_with_expected_fixture


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
                    "input": {key: val for key, val in payload.items() if key != "name"},
                    "loc": ["body", "name"],
                    "msg": "Field required",
                    "type": "missing",
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
            payload["name"] = f'{self.payload["name"]}_{i}'
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
                "updated_at": None,
                "source": "snowflake",
                "database_details": {
                    "account": "sf_account",
                    "database_name": "sf_database",
                    "schema_name": "sf_schema",
                    "role_name": "TESTING",
                    "warehouse": "sf_warehouse",
                },
            }.items()
        )
        assert "created_at" in response_dict

    @pytest.mark.parametrize("refresh", [True, False])
    def test_list_databases__200(
        self, test_api_client_persistent, create_success_response, mock_get_session, refresh
    ):
        """
        Test list databases
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        databases = ["a", "b", "c"]
        mock_get_session.return_value.list_databases.return_value = databases
        response = test_api_client.post(
            f"{self.base_route}/database?refresh={refresh}", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == databases

    def test_list_databases__401(
        self,
        test_api_client_persistent,
        create_success_response,
        snowflake_connector,
        mock_get_session,
    ):
        """
        Test list databases with invalid credentials
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        credentials_error = CredentialsError("Invalid credentials provided.")
        snowflake_connector.side_effect = CredentialsError
        mock_get_session.return_value.list_databases.side_effect = credentials_error
        response = test_api_client.post(f"{self.base_route}/database", json=feature_store)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {"detail": str(credentials_error)}

    def test_list_schemas__422(self, test_api_client_persistent, create_success_response):
        """
        Test list schemas
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        response = test_api_client.post(f"{self.base_route}/schema", json=feature_store)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "input": None,
                    "loc": ["query", "database_name"],
                    "msg": "Field required",
                    "type": "missing",
                }
            ]
        }

    def test_list_schemas__424(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list schemas with non-existent database
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        mock_get_session.return_value.no_schema_error = ProgrammingError
        mock_get_session.return_value.list_schemas.side_effect = ProgrammingError()
        response = test_api_client.post(
            f"{self.base_route}/schema/?database_name=some_database", json=feature_store
        )
        assert response.status_code == HTTPStatus.FAILED_DEPENDENCY
        assert response.json() == {
            "detail": "Database not found. Please specify a valid database name.",
        }

    @pytest.mark.parametrize("refresh", [True, False])
    def test_list_schemas__200(
        self, test_api_client_persistent, create_success_response, mock_get_session, refresh
    ):
        """
        Test list schemas
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        mock_get_session.return_value.list_databases.return_value = ["x"]
        schemas = ["a", "b", "c"]
        mock_get_session.return_value.list_schemas.return_value = schemas
        response = test_api_client.post(
            f"{self.base_route}/schema?database_name=X&refresh={refresh}", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == schemas

    def test_list_tables__422(self, test_api_client_persistent, create_success_response):
        """
        Test list tables
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        response = test_api_client.post(f"{self.base_route}/table", json=feature_store)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "input": None,
                    "loc": ["query", "database_name"],
                    "msg": "Field required",
                    "type": "missing",
                },
                {
                    "input": None,
                    "loc": ["query", "schema_name"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ]
        }

    def test_list_tables__424(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list tables with non-existent schema
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        mock_get_session.return_value.no_schema_error = ProgrammingError
        mock_get_session.return_value.list_tables.side_effect = ProgrammingError()
        response = test_api_client.post(
            f"{self.base_route}/table/?database_name=database&schema_name=some_schema",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.FAILED_DEPENDENCY
        assert response.json() == {
            "detail": "Schema not found. Please specify a valid schema name.",
        }

    @pytest.mark.parametrize("refresh", [True, False])
    def test_list_tables__200(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
        mock_is_featurebyte_schema,
        refresh,
    ):
        """
        Test list tables
        """
        _ = mock_is_featurebyte_schema
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        mock_get_session.return_value.list_databases.return_value = ["x"]
        mock_get_session.return_value.list_schemas.return_value = ["y"]
        tables = ["a", "b", "c", "__d", "__e", "__f"]
        mock_get_session.return_value.list_tables.return_value = [
            TableSpec(name=tb) for tb in tables
        ]
        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh={refresh}",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.text
        # tables with names that has a "__" prefix should be excluded
        assert response.json() == tables[:3]

    def test_list_columns_422(self, test_api_client_persistent, create_success_response):
        """
        Test list columns
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        response = test_api_client.post(f"{self.base_route}/column", json=feature_store)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "input": None,
                    "loc": ["query", "database_name"],
                    "msg": "Field required",
                    "type": "missing",
                },
                {
                    "input": None,
                    "loc": ["query", "schema_name"],
                    "msg": "Field required",
                    "type": "missing",
                },
                {
                    "input": None,
                    "loc": ["query", "table_name"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ]
        }

    def test_list_columns__424(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list columns with non-existent table
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        mock_get_session.return_value.no_schema_error = ProgrammingError
        mock_get_session.return_value.list_table_schema.side_effect = ProgrammingError()
        response = test_api_client.post(
            f"{self.base_route}/column?database_name=database&schema_name=schema&table_name=some_table",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.FAILED_DEPENDENCY
        assert response.json() == {
            "detail": "Table not found. Please specify a valid table name.",
        }

    @pytest.mark.parametrize("refresh", [True, False])
    def test_list_columns__200(
        self, test_api_client_persistent, create_success_response, mock_get_session, refresh
    ):
        """
        Test list columns
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        mock_get_session.return_value.list_databases.return_value = ["x"]
        mock_get_session.return_value.list_schemas.return_value = ["y"]
        mock_get_session.return_value.list_tables.return_value = [TableSpec(name="z")]
        columns = {
            "a": ColumnSpecWithDescription(name="a", dtype="TIMESTAMP"),
            "b": ColumnSpecWithDescription(name="b", dtype="INT"),
            "c": ColumnSpecWithDescription(name="c", dtype="BOOL"),
        }
        mock_get_session.return_value.list_table_schema.return_value = columns
        response = test_api_client.post(
            f"{self.base_route}/column?database_name=X&schema_name=Y&table_name=Z&refresh={refresh}",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == [
            {
                "critical_data_info": None,
                "description": None,
                "dtype": "TIMESTAMP",
                "entity_id": None,
                "name": "a",
                "semantic_id": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "critical_data_info": None,
                "description": None,
                "dtype": "INT",
                "entity_id": None,
                "name": "b",
                "semantic_id": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
            {
                "critical_data_info": None,
                "description": None,
                "dtype": "BOOL",
                "entity_id": None,
                "name": "c",
                "semantic_id": None,
                "dtype_metadata": None,
                "partition_metadata": None,
            },
        ]

    @pytest.fixture(name="data_sample_payload")
    def data_sample_payload_fixture(
        self, test_api_client_persistent, create_success_response, snowflake_feature_store_params
    ):
        """Payload for table sample"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent
        self.activate_catalog(test_api_client)
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        response = test_api_client.post("/event_table", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        data_response_dict = response.json()
        snowflake_feature_store = FeatureStore(**snowflake_feature_store_params, type="snowflake")
        return FeatureStoreSample(
            graph={
                "edges": [{"source": "input_1", "target": "project_1"}],
                "nodes": [
                    {
                        "name": "input_1",
                        "output_type": "frame",
                        "parameters": {
                            "type": data_response_dict["type"],
                            "id": "6332fdb21e8f0eeccc414512",
                            "columns": [
                                {"name": "col_int", "dtype": "INT"},
                                {"name": "col_float", "dtype": "FLOAT"},
                                {"name": "col_char", "dtype": "CHAR"},
                                {"name": "col_text", "dtype": "VARCHAR"},
                                {"name": "col_binary", "dtype": "BINARY"},
                                {"name": "col_boolean", "dtype": "BOOL"},
                                {"name": "event_timestamp", "dtype": "TIMESTAMP"},
                                {"name": "created_at", "dtype": "TIMESTAMP"},
                                {"name": "cust_id", "dtype": "VARCHAR"},
                            ],
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_table",
                            },
                            "feature_store_details": snowflake_feature_store.json_dict(),
                            "timestamp_column": "event_timestamp",
                            "id_column": "event_timestamp",
                        },
                        "type": "input",
                    },
                    {
                        "name": "project_1",
                        "output_type": "frame",
                        "parameters": {
                            "columns": [
                                "col_int",
                                "col_float",
                                "col_char",
                                "col_text",
                                "col_binary",
                                "col_boolean",
                                "event_timestamp",
                                "created_at",
                                "cust_id",
                            ]
                        },
                        "type": "project",
                    },
                ],
            },
            node_name="project_1",
            from_timestamp="2012-11-24T11:00:00",
            to_timestamp="2019-11-24T11:00:00",
            timestamp_column="event_timestamp",
        ).json_dict()

    def test_sample_200(
        self, test_api_client_persistent, data_sample_payload, mock_get_session, source_info
    ):
        """Test table sample (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.side_effect = [
            pd.DataFrame([{"count": 100}]),
            expected_df,
        ]
        mock_session.generate_session_unique_id = Mock(return_value="1")
        response = test_api_client.post("/feature_store/sample", json=data_sample_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                SELECT
                  "col_int",
                  "col_float",
                  "col_char",
                  "col_text",
                  "col_binary",
                  "col_boolean",
                  "event_timestamp",
                  "created_at",
                  "cust_id"
                FROM (
                  SELECT
                    CAST(BITAND(RANDOM(1234), 2147483647) AS DOUBLE) / 2147483647.0 AS "prob",
                    "col_int",
                    "col_float",
                    "col_char",
                    "col_text",
                    "col_binary",
                    "col_boolean",
                    "event_timestamp",
                    "created_at",
                    "cust_id"
                  FROM (
                    SELECT
                      "col_int" AS "col_int",
                      "col_float" AS "col_float",
                      "col_char" AS "col_char",
                      CAST("col_text" AS VARCHAR) AS "col_text",
                      "col_binary" AS "col_binary",
                      "col_boolean" AS "col_boolean",
                      IFF(
                        CAST("event_timestamp" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
                        OR CAST("event_timestamp" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
                        NULL,
                        "event_timestamp"
                      ) AS "event_timestamp",
                      IFF(
                        CAST("created_at" AS TIMESTAMP) < CAST('1900-01-01' AS TIMESTAMP)
                        OR CAST("created_at" AS TIMESTAMP) > CAST('2200-01-01' AS TIMESTAMP),
                        NULL,
                        "created_at"
                      ) AS "created_at",
                      CAST("cust_id" AS VARCHAR) AS "cust_id"
                    FROM "sf_database"."sf_schema"."sf_table"
                    WHERE
                      "event_timestamp" >= CAST('2012-11-24T11:00:00' AS TIMESTAMP)
                      AND "event_timestamp" < CAST('2019-11-24T11:00:00' AS TIMESTAMP)
                  )
                )
                WHERE
                  "prob" <= 0.15000000000000002
                ORDER BY
                  "prob"
                LIMIT 10
                """
            ).strip()
        )

    def test_sample_422__no_timestamp_column(self, test_api_client_persistent, data_sample_payload):
        """Test table sample no timestamp column"""
        test_api_client, _ = test_api_client_persistent
        payload = {
            **data_sample_payload,
            "from_timestamp": "2012-11-24T11:00:00",
            "to_timestamp": None,
            "timestamp_column": None,
        }
        response = test_api_client.post("/feature_store/sample", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "ctx": {"error": {}},
                    "input": payload,
                    "loc": ["body"],
                    "msg": "Value error, timestamp_column must be specified.",
                    "type": "value_error",
                }
            ]
        }

    def test_sample_422__invalid_timestamp_range(
        self, test_api_client_persistent, data_sample_payload
    ):
        """Test table sample no timestamp column"""
        test_api_client, _ = test_api_client_persistent
        payload = {
            **data_sample_payload,
            "from_timestamp": "2012-11-24T11:00:00",
            "to_timestamp": "2012-11-20T11:00:00",
        }
        response = test_api_client.post("/feature_store/sample", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "ctx": {"error": {}},
                    "input": payload,
                    "loc": ["body"],
                    "msg": "Value error, from_timestamp must be smaller than to_timestamp.",
                    "type": "value_error",
                }
            ]
        }

    def test_description_200(
        self, test_api_client_persistent, data_sample_payload, mock_get_session, update_fixtures
    ):
        """Test table description (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame(
            {
                "col_float": [
                    "FLOAT",
                    5,
                    1.0,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    0.256,
                    0.00123,
                    0,
                    0.01,
                    0.155,
                    0.357,
                    1.327,
                ],
                "col_text": [
                    "VARCHAR",
                    5,
                    3.0,
                    0.1,
                    0.123,
                    "a",
                    5,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                    np.nan,
                ],
            },
            index=[
                "dtype",
                "unique",
                "%missing",
                "%empty",
                "entropy",
                "top",
                "freq",
                "mean",
                "std",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            ],
        )
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = pd.DataFrame({
            "a_dtype": ["FLOAT"],
            "a_unique": [5],
            "a_%missing": [1.0],
            "a_%empty": [np.nan],
            "a_entropy": [np.nan],
            "a_top": [np.nan],
            "a_freq": [np.nan],
            "a_mean": [0.256],
            "a_std": [0.00123],
            "a_min": [0],
            "a_p25": [0.01],
            "a_p50": [0.155],
            "a_p75": [0.357],
            "a_max": [1.327],
            "a_min_offset": [np.nan],
            "a_max_offset": [np.nan],
            "b_dtype": ["VARCHAR"],
            "b_unique": [5],
            "b_%missing": [3.0],
            "b_%empty": [0.1],
            "b_entropy": [0.123],
            "b_top": ["a"],
            "b_freq": [5],
            "b_mean": [np.nan],
            "b_std": [np.nan],
            "b_min": [np.nan],
            "b_p25": [np.nan],
            "b_p50": [np.nan],
            "b_p75": [np.nan],
            "b_max": [np.nan],
            "b_min_offset": [np.nan],
            "b_max_offset": [np.nan],
        })
        mock_session.generate_session_unique_id = Mock(return_value="1")
        sample_payload = copy.deepcopy(data_sample_payload)
        sample_payload["graph"]["nodes"][1]["parameters"]["columns"] = ["col_float", "col_text"]
        with patch(
            "featurebyte.service.preview.ObjectId",
            return_value=ObjectId("0" * 24),
        ):
            response = test_api_client.post("/feature_store/description", json=sample_payload)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert_frame_equal(dataframe_from_json(response.json()), expected_df, check_dtype=False)

        # check SQL statement
        assert_equal_with_expected_fixture(
            mock_session.execute_query.call_args[0][0],
            "tests/fixtures/expected_describe_request.sql",
            update_fixture=update_fixtures,
        )

    def test_description_200_numeric_only(
        self, test_api_client_persistent, data_sample_payload, mock_get_session
    ):
        """Test table description with numeric columns only (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame(
            {
                "col_float": ["FLOAT", 20, 1.0, 0.256, 0.00123, 0, 0.01, 0.155, 0.357, 1.327],
            },
            index=[
                "dtype",
                "unique",
                "%missing",
                "mean",
                "std",
                "min",
                "25%",
                "50%",
                "75%",
                "max",
            ],
        )
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = pd.DataFrame({
            "a_dtype": ["FLOAT"],
            "a_unique": [20],
            "a_%missing": [1.0],
            "a_%empty": [np.nan],
            "a_entropy": [np.nan],
            "a_top": [np.nan],
            "a_freq": [np.nan],
            "a_mean": [0.256],
            "a_std": [0.00123],
            "a_min": [0],
            "a_p25": [0.01],
            "a_p50": [0.155],
            "a_p75": [0.357],
            "a_max": [1.327],
            "a_min_offset": [np.nan],
            "a_max_offset": [np.nan],
        })
        mock_session.generate_session_unique_id = Mock(return_value="1")
        sample_payload = copy.deepcopy(data_sample_payload)
        sample_payload["graph"]["nodes"][1]["parameters"]["columns"] = ["col_float"]
        response = test_api_client.post("/feature_store/description", json=sample_payload)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert_frame_equal(dataframe_from_json(response.json()), expected_df, check_dtype=False)

    def test_sample_empty_table(
        self, test_api_client_persistent, data_sample_payload, mock_get_session, source_info
    ):
        """Test table sample works with empty table"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({"a": ["a"]})[:0]
        mock_session = mock_get_session.return_value
        mock_session.execute_query.side_effect = [pd.DataFrame([{"count": 0}]), expected_df]
        response = test_api_client.post("/feature_store/sample", json=data_sample_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_shape_200(self, test_api_client_persistent, data_sample_payload, mock_get_session):
        """Test shape (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({"count": [100]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        data_preview = FeatureStorePreview(**data_sample_payload)
        response = test_api_client.post("/feature_store/shape", json=data_preview.json_dict())
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"num_rows": 100, "num_cols": 9}
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                WITH data AS (
                  SELECT
                    "col_int" AS "col_int",
                    "col_float" AS "col_float",
                    "col_char" AS "col_char",
                    "col_text" AS "col_text",
                    "col_binary" AS "col_binary",
                    "col_boolean" AS "col_boolean",
                    "event_timestamp" AS "event_timestamp",
                    "created_at" AS "created_at",
                    "cust_id" AS "cust_id"
                  FROM "sf_database"."sf_schema"."sf_table"
                )
                SELECT
                  COUNT(*) AS "count"
                FROM data
                """
            ).strip()
        )

    @pytest.fixture(name="tabular_source")
    def tabular_source_fixture(self, create_success_response):
        """Tabular source fixture"""
        return TabularSource(
            feature_store_id=create_success_response.json()["_id"],
            table_details=TableDetails(
                database_name="sf_database",
                schema_name="sf_schema",
                table_name="sf_table",
            ),
        )

    @pytest.fixture(name="query_preview")
    def query_preview_fixture(self, create_success_response):
        """Query preview fixture"""
        return FeatureStoreQueryPreview(
            feature_store_id=create_success_response.json()["_id"],
            sql='SELECT *, 1 AS new_col FROM "sf_database"."sf_schema"."sf_table"',
        )

    def test_table_shape_200(self, test_api_client_persistent, tabular_source, mock_get_session):
        """Test table shape (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({"row_count": [100]})
        mock_session = mock_get_session.return_value
        mock_session.list_table_schema.return_value = {
            f"col_{i}": {"name": f"col_{i}"} for i in range(9)
        }
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        response = test_api_client.post(
            "/feature_store/table_shape", json=tabular_source.json_dict()
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == {"num_rows": 100, "num_cols": 9}
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                SELECT
                  COUNT(*) AS "row_count"
                FROM "sf_database"."sf_schema"."sf_table"
                """
            ).strip()
        )

    def test_table_preview_200(self, test_api_client_persistent, tabular_source, mock_get_session):
        """Test table preview (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({
            "col_int": [1, 2, 3],
        })
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        response = test_api_client.post(
            "/feature_store/table_preview?limit=3", json=tabular_source.json_dict()
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == dataframe_to_json(expected_df)
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                SELECT
                  *
                FROM "sf_database"."sf_schema"."sf_table"
                LIMIT 3
                """
            ).strip()
        )

    def test_sql_preview_200(self, test_api_client_persistent, query_preview, mock_get_session):
        """Test sql preview (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({
            "col_int": [1, 2, 3],
        })
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        response = test_api_client.post(
            "/feature_store/sql_preview?limit=3", json=query_preview.json_dict()
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == dataframe_to_json(expected_df)
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
                SELECT
                  *,
                  1 AS new_col
                FROM "sf_database"."sf_schema"."sf_table"
                LIMIT 3
                """
            ).strip()
        )

    def test_sql_preview_422(self, test_api_client_persistent, query_preview):
        """Test sql preview (failure duw to invalid SQL)"""
        test_api_client, _ = test_api_client_persistent

        query_preview.sql = "DROP TABLE sf_database.sf_schema.sf_table"
        response = test_api_client.post(
            "/feature_store/sql_preview", json=query_preview.json_dict()
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {"detail": "SQL must be a SELECT statement."}

    @pytest.mark.asyncio
    async def test_credentials_stored(self, test_api_client_persistent):
        """
        Test that credentials are stored in the database upon successful Feature Store creation
        """
        test_api_client, persistent = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        feature_store_id = self.payload["_id"]

        # check that no credential exists for the feature store before creation
        credential = await persistent.find_one(
            collection_name=CredentialModel.collection_name(),
            query_filter={"feature_store_id": ObjectId(feature_store_id)},
        )
        assert not credential

        # create feature store
        payload = copy.deepcopy(self.payload)
        response = test_api_client.post(f"{self.base_route}", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED
        assert response_dict["_id"] == feature_store_id

        # check that no credential exists for the feature store before creation
        credential = await persistent.find_one(
            collection_name=CredentialModel.collection_name(),
            query_filter={"feature_store_id": ObjectId(feature_store_id)},
        )
        assert credential

    def test_create_with_credentials(self, test_api_client_persistent):
        """
        Test that credentials are stored in the database upon successful Feature Store creation
        """
        test_api_client, _ = test_api_client_persistent
        payload = copy.deepcopy(self.payload)
        payload["database_credential"] = UsernamePasswordCredential(
            username="username", password="password"
        ).json_dict()
        payload["storage_credential"] = S3StorageCredential(
            s3_access_key_id="s3_access_key_id",
            s3_secret_access_key="s3_secret_access_key",
        ).json_dict()
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.CREATED

        # check credentials are stores
        response = test_api_client.get(f"/credential?feature_store_id={payload['_id']}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["total"] == 1
        credential_dict = response_dict["data"][0]
        assert credential_dict["feature_store_id"] == payload["_id"]
        assert credential_dict["database_credential"] == {
            "type": "USERNAME_PASSWORD",
            "username": "********",
            "password": "********",
        }
        assert credential_dict["storage_credential"] == {
            "type": "S3",
            "s3_access_key_id": "********",
            "s3_secret_access_key": "********",
        }

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete feature store"""
        test_api_client, _ = test_api_client_persistent
        feature_store_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"{self.base_route}/{feature_store_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check that feature store is deleted
        response = test_api_client.get(f"{self.base_route}/{feature_store_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_422(self, test_api_client_persistent, create_success_response):
        """Test delete feature store (unsuccessful)"""
        test_api_client, _ = test_api_client_persistent
        feature_store_id = create_success_response.json()["_id"]

        # create a catalog using the feature store
        catalog_payload = self.load_payload("tests/fixtures/request_payloads/catalog.json")
        response = test_api_client.post("/catalog", json=catalog_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        catalog_id = response.json()["_id"]

        # attempt to delete the feature store
        response = test_api_client.delete(f"{self.base_route}/{feature_store_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "FeatureStore is referenced by Catalog: grocery"

        # soft delete the catalog
        response = test_api_client.delete(f"/catalog/{catalog_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # attempt to delete the feature store again
        response = test_api_client.delete(f"{self.base_route}/{feature_store_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "FeatureStore is referenced by Catalog: grocery (soft deleted). "
            "Please try again after the Catalog is permanently deleted."
        )

    def test_update_database_details_200(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Test update database details (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        update_payload = {
            "warehouse": "new_warehouse",
        }
        response = test_api_client.patch(f"{self.base_route}/{doc_id}/details", json=update_payload)
        assert response.status_code == HTTPStatus.OK, response.json()

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["_id"] == doc_id
        assert datetime.fromisoformat(response_dict["updated_at"]) < datetime.utcnow()
        assert response_dict["user_id"] == str(user_id)
        assert response_dict["name"] == self.payload["name"]
        assert response_dict["details"]["warehouse"] == update_payload["warehouse"]

    def test_update_database_details_422(
        self, test_api_client_persistent, create_success_response, snowflake_execute_query
    ):
        """Test update database details (failure)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        snowflake_execute_query.side_effect = ProgrammingError("Some Error")

        update_payload = {
            "warehouse": "new_warehouse",
        }
        response = test_api_client.patch(f"{self.base_route}/{doc_id}/details", json=update_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json() == {"detail": "Invalid details: Some Error"}

    def test_gcs_credentials(self, test_api_client_persistent):
        """
        Test GCS storage credential with string for service account info accepted
        """
        test_api_client, _ = test_api_client_persistent
        payload = {
            "database_credential": None,
            "details": {
                "account": "sf_account",
                "database_name": "sf_database",
                "role_name": "TESTING",
                "schema_name": "sf_schema",
                "warehouse": "sf_warehouse",
            },
            "name": "sf_featurestore",
            "storage_credential": {
                "type": "GCS",
                "service_account_info": json.dumps({
                    "type": "service_account",
                    "private_key": "private_key",
                }),
            },
            "type": "spark",
        }
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == 201

    def test_update_feature_store_200(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Test update feature store (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        update_payload = {
            "max_query_concurrency": 5,
        }
        response = test_api_client.patch(f"{self.base_route}/{doc_id}", json=update_payload)
        assert response.status_code == HTTPStatus.OK, response.json()

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["_id"] == doc_id
        assert datetime.fromisoformat(response_dict["updated_at"]) < datetime.utcnow()
        assert response_dict["user_id"] == str(user_id)
        assert response_dict["name"] == self.payload["name"]
        assert response_dict["max_query_concurrency"] == update_payload["max_query_concurrency"]

    def test_update_feature_store_422(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Test update feature store (failure)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        update_payload = {
            "max_query_concurrency": 0,
        }
        response = test_api_client.patch(f"{self.base_route}/{doc_id}", json=update_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json() == {
            "detail": [
                {
                    "type": "greater_than",
                    "loc": ["body", "max_query_concurrency"],
                    "msg": "Input should be greater than 0",
                    "input": 0,
                    "ctx": {"gt": 0},
                }
            ]
        }

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["_id"] == doc_id
        assert response_dict["updated_at"] is None
        assert response_dict["user_id"] == str(user_id)
        assert response_dict["name"] == self.payload["name"]
        assert response_dict["max_query_concurrency"] is None

    @patch("featurebyte.service.session_manager.SessionManagerService.list_compute_options")
    def test_list_compute_options_200(
        self, mock_list_compute_options, test_api_client_persistent, create_success_response
    ):
        """Test list compute options (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        mock_list_compute_options.return_value = [
            ComputeOption(name="option1", value="Option 1"),
            ComputeOption(name="option2", value="Option 2"),
        ]

        response = test_api_client.get(f"{self.base_route}/{doc_id}/compute_option")
        assert response.status_code == HTTPStatus.OK, response.json()
        response_dict = response.json()
        assert len(response_dict) == 2
        assert response_dict == [
            {
                "name": "option1",
                "value": "Option 1",
                "details": {},
            },
            {
                "name": "option2",
                "value": "Option 2",
                "details": {},
            },
        ]

    def test_no_cache_200(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """Test list without cache(success)"""
        test_api_client, _ = test_api_client_persistent
        feature_store = create_success_response.json()
        doc_id = feature_store["_id"]

        mock_get_session.return_value.list_databases.return_value = ["x"]
        mock_get_session.return_value.list_schemas.return_value = ["y"]
        tables = ["a", "b", "c"]
        mock_get_session.return_value.list_tables.return_value = [
            TableSpec(name=tb) for tb in tables
        ]

        response = test_api_client.post(
            f"{self.base_route}/database?refresh=False", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == ["x"]

        response = test_api_client.post(
            f"{self.base_route}/schema?database_name=X&refresh=False", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == ["y"]

        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh=False",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == tables[:3]

        # check cache works
        mock_get_session.return_value.list_databases.return_value = []
        mock_get_session.return_value.list_schemas.return_value = []
        mock_get_session.return_value.list_tables.return_value = []
        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh=False",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == tables[:3]

        # expect cache to be cleared
        response = test_api_client.post(
            f"{self.base_route}/database?refresh=True", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == []

        response = test_api_client.post(
            f"{self.base_route}/schema?database_name=X&refresh=True", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == []

        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh=True",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == []

    def test_clear_cache_200(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """Test clear cache (success)"""
        test_api_client, _ = test_api_client_persistent
        feature_store = create_success_response.json()
        doc_id = feature_store["_id"]

        mock_get_session.return_value.list_databases.return_value = ["x"]
        mock_get_session.return_value.list_schemas.return_value = ["y"]
        tables = ["a", "b", "c"]
        mock_get_session.return_value.list_tables.return_value = [
            TableSpec(name=tb) for tb in tables
        ]

        response = test_api_client.post(
            f"{self.base_route}/database?refresh=False", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == ["x"]

        response = test_api_client.post(
            f"{self.base_route}/schema?database_name=X&refresh=False", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == ["y"]

        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh=False",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == tables[:3]

        # check cache works
        mock_get_session.return_value.list_databases.return_value = []
        mock_get_session.return_value.list_schemas.return_value = []
        mock_get_session.return_value.list_tables.return_value = []
        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh=False",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == tables[:3]

        # clear cache
        response = test_api_client.delete(f"{self.base_route}/{doc_id}/cache")
        assert response.status_code == HTTPStatus.OK, response.json()

        # expect cache to be cleared
        response = test_api_client.post(
            f"{self.base_route}/database?refresh=False", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == []

        response = test_api_client.post(
            f"{self.base_route}/schema?database_name=X&refresh=False", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == []

        response = test_api_client.post(
            f"{self.base_route}/table?database_name=X&schema_name=Y&refresh=False",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json() == []
