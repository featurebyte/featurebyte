"""
Test for FeatureStore route
"""
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.exception import CredentialsError
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

    @pytest.fixture(name="mock_get_session")
    def get_mock_get_session_fixture(self):
        """
        Return
        """
        with patch(
            "featurebyte.service.session_manager.SessionManager.get_session"
        ) as mocked_get_session:
            yield mocked_get_session

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

    @pytest.mark.usefixtures("mock_get_session")
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

    @pytest.mark.usefixtures("mock_get_session")
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

    @pytest.mark.usefixtures("mock_get_session")
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
