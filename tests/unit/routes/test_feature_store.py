"""
Test for FeatureStore route
"""
import copy
import textwrap
from http import HTTPStatus
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte import FeatureStore
from featurebyte.common.utils import dataframe_from_json
from featurebyte.exception import CredentialsError
from featurebyte.models.credential import (
    CredentialModel,
    S3StorageCredential,
    UsernamePasswordCredential,
)
from featurebyte.schema.feature_store import FeatureStoreSample
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
                    "database": "sf_database",
                    "sf_schema": "sf_schema",
                    "warehouse": "sf_warehouse",
                },
            }.items()
        )
        assert "created_at" in response_dict

    def test_list_databases__200(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list databases
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        databases = ["a", "b", "c"]
        mock_get_session.return_value.list_databases.return_value = databases
        response = test_api_client.post(f"{self.base_route}/database", json=feature_store)
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
                    "loc": ["query", "database_name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
        }

    def test_list_schemas__200(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list schemas
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        schemas = ["a", "b", "c"]
        mock_get_session.return_value.list_schemas.return_value = schemas
        response = test_api_client.post(
            f"{self.base_route}/schema?database_name=x", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == schemas

    def test_list_tables_422(self, test_api_client_persistent, create_success_response):
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
                    "loc": ["query", "database_name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["query", "schema_name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        }

    def test_list_tables__200(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list tables
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        tables = ["a", "b", "c"]
        mock_get_session.return_value.list_tables.return_value = tables
        response = test_api_client.post(
            f"{self.base_route}/table?database_name=x&schema_name=y", json=feature_store
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == tables

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
                    "loc": ["query", "database_name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["query", "schema_name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["query", "table_name"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        }

    def test_list_columns__200(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """
        Test list columns
        """
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.CREATED
        feature_store = create_success_response.json()

        columns = {"a": "TIMESTAMP", "b": "INT", "c": "BOOL"}
        mock_get_session.return_value.list_table_schema.return_value = columns
        response = test_api_client.post(
            f"{self.base_route}/column?database_name=x&schema_name=y&table_name=z",
            json=feature_store,
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == [
            {"name": "a", "dtype": "TIMESTAMP"},
            {"name": "b", "dtype": "INT"},
            {"name": "c", "dtype": "BOOL"},
        ]

    @pytest.fixture(name="data_sample_payload")
    def data_sample_payload_fixture(
        self, test_api_client_persistent, create_success_response, snowflake_feature_store_params
    ):
        """Payload for table sample"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        response = test_api_client.post("/event_table", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        data_response_dict = response.json()
        snowflake_feature_store = FeatureStore(**snowflake_feature_store_params, type="snowflake")
        return FeatureStoreSample(
            feature_store_name=snowflake_feature_store.name,
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

    def test_sample_200(self, test_api_client_persistent, data_sample_payload, mock_get_session):
        """Test table sample (success)"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")
        response = test_api_client.post("/feature_store/sample", json=data_sample_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
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
                WHERE
                  "event_timestamp" >= CAST('2012-11-24T11:00:00' AS TIMESTAMPNTZ)
                  AND "event_timestamp" < CAST('2019-11-24T11:00:00' AS TIMESTAMPNTZ)
                ORDER BY
                  RANDOM(1234)
                LIMIT 10
                """
            ).strip()
        )

    def test_sample_422__no_timestamp_column(self, test_api_client_persistent, data_sample_payload):
        """Test table sample no timestamp column"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(
            "/feature_store/sample",
            json={
                **data_sample_payload,
                "from_timestamp": "2012-11-24T11:00:00",
                "to_timestamp": None,
                "timestamp_column": None,
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "loc": ["body", "__root__"],
                    "msg": "timestamp_column must be specified.",
                    "type": "value_error",
                }
            ]
        }

    def test_sample_422__invalid_timestamp_range(
        self, test_api_client_persistent, data_sample_payload
    ):
        """Test table sample no timestamp column"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(
            "/feature_store/sample",
            json={
                **data_sample_payload,
                "from_timestamp": "2012-11-24T11:00:00",
                "to_timestamp": "2012-11-20T11:00:00",
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "loc": ["body", "__root__"],
                    "msg": "from_timestamp must be smaller than to_timestamp.",
                    "type": "assertion_error",
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
        mock_session.execute_query.return_value = pd.DataFrame(
            {
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
            }
        )
        mock_session.generate_session_unique_id = Mock(return_value="1")
        sample_payload = copy.deepcopy(data_sample_payload)
        sample_payload["graph"]["nodes"][1]["parameters"]["columns"] = ["col_float", "col_text"]
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
        mock_session.execute_query.return_value = pd.DataFrame(
            {
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
            }
        )
        mock_session.generate_session_unique_id = Mock(return_value="1")
        sample_payload = copy.deepcopy(data_sample_payload)
        sample_payload["graph"]["nodes"][1]["parameters"]["columns"] = ["col_float"]
        response = test_api_client.post("/feature_store/description", json=sample_payload)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert_frame_equal(dataframe_from_json(response.json()), expected_df, check_dtype=False)

    def test_sample_empty_table(
        self, test_api_client_persistent, data_sample_payload, mock_get_session
    ):
        """Test table sample works with empty table"""
        test_api_client, _ = test_api_client_persistent

        expected_df = pd.DataFrame({"a": ["a"]})[:0]
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        response = test_api_client.post("/feature_store/sample", json=data_sample_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

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
        assert credential_dict["database_credential_type"] == "USERNAME_PASSWORD"
        assert credential_dict["storage_credential_type"] == "S3"
