"""
Tests for BatchFeatureTable routes
"""

import copy
from http import HTTPStatus
from unittest.mock import patch

import freezegun
import pytest
from bson.objectid import ObjectId

from featurebyte import SourceType
from featurebyte.exception import SchemaNotFoundError
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.schema.batch_feature_table import (
    BatchExternalFeatureTableCreate,
    OutputTableInfo,
)
from tests.unit.routes.base import BaseMaterializedTableTestSuite
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


class TestBatchFeatureTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for BatchFeatureTable route
    """

    class_name = "BatchFeatureTable"
    base_route = "/batch_feature_table"
    payload = BaseMaterializedTableTestSuite.load_payload(
        "tests/fixtures/request_payloads/batch_feature_table.json"
    )
    random_id = str(ObjectId())

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'BatchFeatureTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `BatchFeatureTable.get(name="{payload["name"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'BatchFeatureTable (name: "{payload["name"]}") already exists. '
            f'Get the existing object by `BatchFeatureTable.get(name="{payload["name"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "random_name",
                "batch_request_table_id": random_id,
            },
            f'BatchRequestTable (id: "{random_id}") not found. Please save the BatchRequestTable object first.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "random_name", "deployment_id": random_id},
            f'Deployment (id: "{random_id}") not found. Please save the Deployment object first.',
        ),
    ]

    @pytest.fixture(autouse=True)
    def mock_online_enable_service_update_data_warehouse(self, mock_deployment_flow):
        """Mock update_data_warehouse method in OnlineEnableService to make it a no-op"""
        _ = mock_deployment_flow
        with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
            yield

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("context", "context"),
            ("batch_request_table", "batch_request_table"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
            ("deployment", "deployment"),
        ]
        catalog_id = api_client.get("/catalog").json()["data"][0]["_id"]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            if api_object in {"batch_request_table", "deployment"}:
                response = self.wait_for_results(api_client, response)
                assert response.json()["status"] == "SUCCESS", response.json()["traceback"]
            else:
                assert response.status_code == HTTPStatus.CREATED

            if api_object == "feature":
                self.make_feature_production_ready(api_client, response.json()["_id"], catalog_id)
            if api_object == "deployment":
                deployment_id = response.json()["payload"]["output_document_id"]
                self.update_deployment_enabled(api_client, deployment_id, catalog_id)

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def test_create_422__batch_request_table_failed_validation_check(
        self,
        test_api_client_persistent,
        snowflake_execute_query_invalid_batch_request_table,
    ):
        """Test create 422 for batch request table failed validation check"""
        _ = snowflake_execute_query_invalid_batch_request_table
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # check that columns_info is empty as we are mocking the query
        batch_request_table_id = self.payload["batch_request_table_id"]
        response = test_api_client.get(f"/batch_request_table/{batch_request_table_id}")
        assert response.json()["columns_info"] == []

        # check that create fails
        response = test_api_client.post(self.base_route, json=self.payload)

        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            'Required entities are not provided in the request: customer (serving name: "cust_id")'
        )

    @pytest.mark.asyncio
    async def test_batch_request_table_delete_422__batch_request_table_failed_validation_check(
        self, test_api_client_persistent, create_success_response, user_id, default_catalog_id
    ):
        """Test delete 422 for batch request table failed validation check"""
        test_api_client, persistent = test_api_client_persistent
        create_success_response_dict = create_success_response.json()

        # insert another document to batch feature table to make sure the query filter is correct
        payload = copy.deepcopy(self.payload)
        payload["_id"] = ObjectId()
        payload["name"] = "random_name"
        await persistent.insert_one(
            collection_name="batch_feature_table",
            document={
                **payload,
                "catalog_id": ObjectId(default_catalog_id),
                "user_id": user_id,
                "batch_request_table_id": ObjectId(),  # different batch request table id
                "columns_info": [],
                "num_rows": 500,
                "location": create_success_response_dict["location"],
            },
            user_id=user_id,
        )
        response = test_api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["total"] == 2

        # try to delete batch request table
        batch_request_table_id = self.payload["batch_request_table_id"]
        response = test_api_client.delete(f"/batch_request_table/{batch_request_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            "BatchRequestTable is referenced by BatchFeatureTable: batch_feature_table"
        )

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": self.payload["name"],
            "deployment_name": "my_deployment",
            "batch_request_table_name": "batch_request_table",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "description": None,
        }

    def test_delete_deployment(
        self, test_api_client_persistent, create_success_response, default_catalog_id
    ):
        """Test delete deployment used by batch feature table"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # attempt to delete deployment
        deployment_id = response.json()["deployment_id"]
        response = test_api_client.delete(f"/deployment/{deployment_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Only disabled deployment can be deleted."

        # disable the deployment first
        self.update_deployment_enabled(
            test_api_client, deployment_id, default_catalog_id, enabled=False
        )
        response = test_api_client.delete(f"/deployment/{deployment_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Deployment is referenced by BatchFeatureTable: batch_feature_table"
        )

    def test_create_success_from_request_input(self, test_api_client_persistent):
        """Test create success from request input"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = self.load_payload(
            "tests/fixtures/request_payloads/batch_feature_table_with_request_input.json"
        )
        id_before = payload["_id"]
        response = self.post(test_api_client, payload)

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()
        assert response_dict["_id"] == id_before
        assert response_dict["name"] == "batch_feature_table_with_request_input"

    def test_create_fails_multiple_request_inputs(self, test_api_client_persistent):
        """Test create fails with multiple request inputs"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = self.load_payload(
            "tests/fixtures/request_payloads/batch_feature_table_with_request_input.json"
        )
        payload["batch_request_table_id"] = str(ObjectId())
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        response_dict = response.json()
        assert response_dict["detail"] == [
            {
                "type": "value_error",
                "loc": ["body"],
                "msg": "Value error, Only one of batch_request_table_id or request_input must be provided",
                "input": payload,
                "ctx": {"error": {}},
            }
        ]

    def test_create_fails_no_request_input(self, test_api_client_persistent):
        """Test create fails with no request input"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = self.load_payload("tests/fixtures/request_payloads/batch_feature_table.json")
        payload["batch_request_table_id"] = None
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        response_dict = response.json()
        assert response_dict["detail"] == [
            {
                "type": "value_error",
                "loc": ["body"],
                "msg": "Value error, Either batch_request_table_id or request_input must be provided",
                "input": payload,
                "ctx": {"error": {}},
            }
        ]

    @pytest.mark.asyncio
    async def test_recreate_fails_for_older_request_table(
        self, test_api_client_persistent, create_success_response
    ):
        """Test recreate from existing feature table using request table success"""
        test_api_client, persistent = test_api_client_persistent
        id_before = create_success_response.json()["_id"]

        # remove request_input to emulate older records
        await persistent.update_one(
            collection_name="batch_feature_table",
            query_filter={"_id": ObjectId(id_before)},
            update={"$unset": {"request_input": ""}},
            user_id=None,
        )

        response = test_api_client.post(
            f"{self.base_route}/{id_before}",
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        response_dict = response.json()
        assert response_dict["detail"] == "Request input not found for the batch feature table"

    def test_recreate_success_from_request_table(
        self, test_api_client_persistent, create_success_response
    ):
        """Test recreate from existing feature table using request table success"""
        # recreate the batch feature table
        test_api_client, _ = test_api_client_persistent
        id_before = create_success_response.json()["_id"]

        with freezegun.freeze_time("2024-02-13"):
            response = test_api_client.post(
                f"{self.base_route}/{id_before}",
            )

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        recreate_response_dict = response.json()
        assert recreate_response_dict["name"] == "batch_feature_table [2024-02-13T00:00:00]"
        assert recreate_response_dict["parent_batch_feature_table_name"] == "batch_feature_table"

        # should no longer reference the original batch request table
        assert recreate_response_dict["batch_request_table_id"] is None

    def test_recreate_success_from_request_input(self, test_api_client_persistent):
        """Test recreate from existing feature table using request input success"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = self.load_payload(
            "tests/fixtures/request_payloads/batch_feature_table_with_request_input.json"
        )
        id_before = payload["_id"]
        response = self.post(test_api_client, payload)

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()
        assert response_dict["_id"] == id_before
        assert response_dict["name"] == "batch_feature_table_with_request_input"

        # recreate the batch feature table
        with freezegun.freeze_time("2024-02-13"):
            response = test_api_client.post(
                f"{self.base_route}/{id_before}",
            )

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        recreate_response_dict = response.json()
        assert (
            recreate_response_dict["name"]
            == "batch_feature_table_with_request_input [2024-02-13T00:00:00]"
        )
        assert (
            recreate_response_dict["parent_batch_feature_table_name"]
            == "batch_feature_table_with_request_input"
        )
        id_recreated = recreate_response_dict["_id"]

        # recreate the batch feature table from the recreated batch feature table
        with freezegun.freeze_time("2024-02-14"):
            response = test_api_client.post(
                f"{self.base_route}/{id_recreated}",
            )

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        recreate_response_dict = response.json()
        assert (
            recreate_response_dict["name"]
            == "batch_feature_table_with_request_input [2024-02-14T00:00:00]"
        )
        assert (
            recreate_response_dict["parent_batch_feature_table_name"]
            == "batch_feature_table_with_request_input"
        )

    @pytest.fixture(name="payload_for_external_feature_table")
    def payload_for_external_feature_table_fixture(self):
        payload = self.load_payload(
            "tests/fixtures/request_payloads/batch_feature_table_with_request_input.json"
        )
        payload["point_in_time"] = "2025-06-19T03:00:00"
        payload = BatchExternalFeatureTableCreate(
            **payload,
            output_table_info=OutputTableInfo(
                name='"db"."schema"."batch_prediction_table"',
                snapshot_date="2025-06-19",
            ),
        )
        payload.request_input.columns = ["cust_id"]
        return payload.json_dict()

    def test_create_fail_external_feature_table_invalid_table_name(
        self, test_api_client_persistent, payload_for_external_feature_table
    ):
        """Test create fail append to table invalid table name"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload_for_external_feature_table["output_table_info"]["name"] = (
            "`db`.`schema`.`batch_prediction_table`"
        )
        response = test_api_client.post(
            f"{self.base_route}/feature_table", json=payload_for_external_feature_table
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Invalid output table name: `db`.`schema`.`batch_prediction_table`"
        )

    @patch("featurebyte.service.batch_feature_table.FeatureStoreWarehouseService.list_tables")
    def test_create_fail_external_feature_table_schema_does_not_exist(
        self, mock_list_tables, test_api_client_persistent, payload_for_external_feature_table
    ):
        """Test create fail append to table schema does not exist"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        mock_list_tables.side_effect = SchemaNotFoundError

        response = test_api_client.post(
            f"{self.base_route}/feature_table", json=payload_for_external_feature_table
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Invalid output table name: schema not found"

    @patch("featurebyte.service.batch_feature_table.FeatureStoreWarehouseService.list_columns")
    def test_create_fail_external_feature_table_missing_columns(
        self, mock_list_columns, test_api_client_persistent, payload_for_external_feature_table
    ):
        """Test create fail append to table missing columns"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        mock_list_columns.return_value = [
            ColumnSpecWithDescription(
                name="cust_id",
                dtype="VARCHAR",
            ),
        ]

        response = test_api_client.post(
            f"{self.base_route}/feature_table", json=payload_for_external_feature_table
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Output table missing required columns: POINT_IN_TIME, snapshot_date, sum_30m"
        )

    @patch("featurebyte.service.batch_feature_table.FeatureStoreWarehouseService.list_columns")
    def test_create_fail_external_feature_table_mismatch_dtypes(
        self, mock_list_columns, test_api_client_persistent, payload_for_external_feature_table
    ):
        """Test create fail append to table mismatch column dtypes"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        mock_list_columns.return_value = [
            ColumnSpecWithDescription(
                name="POINT_IN_TIME",
                dtype="TIMESTAMP",
            ),
            ColumnSpecWithDescription(
                name="snapshot_date",
                dtype="DATE",
            ),
            ColumnSpecWithDescription(
                name="cust_id",
                dtype="INT",
            ),
            ColumnSpecWithDescription(
                name="sum_30m",
                dtype="FLOAT",
            ),
        ]

        response = test_api_client.post(
            f"{self.base_route}/feature_table", json=payload_for_external_feature_table
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Output table column type mismatch: snapshot_date (expected VARCHAR, got DATE)"
        )

    @pytest.fixture(name="output_table_list_columns")
    def output_table_list_columns_fixture(self):
        with patch(
            "featurebyte.service.batch_feature_table.FeatureStoreWarehouseService.list_columns"
        ) as mock_list_columns:
            mock_list_columns.return_value = [
                ColumnSpecWithDescription(
                    name="POINT_IN_TIME",
                    dtype="TIMESTAMP",
                ),
                ColumnSpecWithDescription(
                    name="snapshot_date",
                    dtype="VARCHAR",
                ),
                ColumnSpecWithDescription(
                    name="cust_id",
                    dtype="INT",
                ),
                ColumnSpecWithDescription(
                    name="sum_30m",
                    dtype="FLOAT",
                ),
            ]
            yield mock_list_columns

    @patch("featurebyte.session.snowflake.SnowflakeSession.execute_query_long_running")
    def test_create_success_external_feature_table_new_table(
        self,
        mock_execute_query_long_running,
        test_api_client_persistent,
        output_table_list_columns,
        payload_for_external_feature_table,
        snowflake_execute_query_for_materialized_table,
        mock_snowflake_session,
        update_fixtures,
    ):
        """Test create success append to table"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        mock_execute_query_long_running.side_effect = snowflake_execute_query_for_materialized_table

        payload_for_external_feature_table["output_table_info"]["name"] = (
            '"db"."schema"."new_batch_prediction_table"'
        )
        response = test_api_client.post(
            f"{self.base_route}/feature_table", json=payload_for_external_feature_table
        )
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        mock_snowflake_session.execute_query_long_running = mock_execute_query_long_running
        queries = extract_session_executed_queries(mock_snowflake_session)
        fixture_filename = "tests/fixtures/batch_feature_table_task/expected_batch_external_feature_table_new_table_queries.sql"
        assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)

    @patch("featurebyte.session.snowflake.SnowflakeSession.execute_query_long_running")
    def test_create_success_external_feature_table_existing_table(
        self,
        mock_execute_query_long_running,
        test_api_client_persistent,
        output_table_list_columns,
        payload_for_external_feature_table,
        snowflake_execute_query_for_materialized_table,
        mock_snowflake_session,
        update_fixtures,
    ):
        """Test create success append to table"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        mock_execute_query_long_running.side_effect = snowflake_execute_query_for_materialized_table

        response = test_api_client.post(
            f"{self.base_route}/feature_table", json=payload_for_external_feature_table
        )
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        mock_snowflake_session.execute_query_long_running = mock_execute_query_long_running
        queries = extract_session_executed_queries(mock_snowflake_session)
        fixture_filename = "tests/fixtures/batch_feature_table_task/expected_batch_external_feature_table_existing_table_queries.sql"
        assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)

    @patch("featurebyte.session.snowflake.SnowflakeSession.execute_query_long_running")
    def test_create_success_external_feature_table_new_databricks_table(
        self,
        mock_execute_query_long_running,
        test_api_client_persistent,
        output_table_list_columns,
        payload_for_external_feature_table,
        snowflake_execute_query_for_materialized_table,
        mock_snowflake_session,
        update_fixtures,
    ):
        """Test create success append to table"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        mock_execute_query_long_running.side_effect = snowflake_execute_query_for_materialized_table

        payload_for_external_feature_table["output_table_info"]["name"] = (
            '"db"."schema"."new_batch_prediction_table"'
        )
        with patch(
            "featurebyte.service.batch_external_feature_table.BatchExternalFeatureTableService._get_source_type"
        ) as mock_get_source_type:
            mock_get_source_type.return_value = SourceType.DATABRICKS_UNITY
            response = test_api_client.post(
                f"{self.base_route}/feature_table", json=payload_for_external_feature_table
            )
            response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        mock_snowflake_session.execute_query_long_running = mock_execute_query_long_running
        queries = extract_session_executed_queries(mock_snowflake_session)
        fixture_filename = "tests/fixtures/batch_feature_table_task/expected_batch_external_feature_table_new_databricks_table_queries.sql"
        assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)

    @patch("featurebyte.session.snowflake.SnowflakeSession.execute_query_long_running")
    def test_create_success_external_feature_table_existing_databricks_table(
        self,
        mock_execute_query_long_running,
        test_api_client_persistent,
        output_table_list_columns,
        payload_for_external_feature_table,
        snowflake_execute_query_for_materialized_table,
        mock_snowflake_session,
        update_fixtures,
    ):
        """Test create success append to table"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        mock_execute_query_long_running.side_effect = snowflake_execute_query_for_materialized_table

        with patch(
            "featurebyte.service.batch_external_feature_table.BatchExternalFeatureTableService._get_source_type"
        ) as mock_get_source_type:
            mock_get_source_type.return_value = SourceType.DATABRICKS_UNITY
            response = test_api_client.post(
                f"{self.base_route}/feature_table", json=payload_for_external_feature_table
            )
            response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        mock_snowflake_session.execute_query_long_running = mock_execute_query_long_running
        queries = extract_session_executed_queries(mock_snowflake_session)
        fixture_filename = "tests/fixtures/batch_feature_table_task/expected_batch_external_feature_table_existing_databricks_table_queries.sql"
        assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)
