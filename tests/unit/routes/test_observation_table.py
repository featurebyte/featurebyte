"""
Tests for ObservationTable routes
"""

import copy
import os
import tempfile
import textwrap
from http import HTTPStatus
from unittest.mock import patch

import pandas as pd
import pytest
from bson.objectid import ObjectId

from featurebyte import DownSamplingInfo
from featurebyte.schema.observation_table import ObservationTableUpload
from tests.unit.routes.base import BaseMaterializedTableTestSuite
from tests.util.helper import assert_equal_with_expected_fixture, extract_session_executed_queries


class TestObservationTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for ObservationTable route
    """

    class_name = "ObservationTable"
    base_route = "/observation_table"
    payload = BaseMaterializedTableTestSuite.load_payload(
        "tests/fixtures/request_payloads/observation_table.json"
    )
    async_create = True

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'ObservationTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `ObservationTable.get(name="{payload["name"]}")`.',
        ),
    ]

    unknown_context_id = str(ObjectId())
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "name": "new_table",
                "primary_entity_ids": None,
                "context_id": unknown_context_id,
            },
            f'Context (id: "{unknown_context_id}") not found. Please save the Context object first.',
        )
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
            ("context", "context"),
            ("target", "target"),
            ("use_case", "use_case"),
            ("treatment", "treatment"),
            ("context", "context_with_treatment"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

            if api_object == "target":
                # update target type
                target_namespace_id = response.json().get("target_namespace_id")
                response = api_client.patch(
                    f"/target_namespace/{target_namespace_id}",
                    json={"target_type": "regression"},
                )
                assert response.status_code == HTTPStatus.OK, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f"{self.payload['name']}_{i}"
            yield payload

    @pytest.fixture(autouse=True)
    def always_patched_observation_table_service(self, patched_observation_table_service):
        """
        Patch ObservationTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_observation_table_service

    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        with patch("featurebyte.session.base.BaseSession.create_table_as") as mock:
            super().test_create_201__without_specifying_id_field(test_api_client_persistent)

        # check create_table_as expression
        create_table_as_calls = mock.call_args_list
        expected_non_missing_expr = textwrap.dedent("""
        SELECT
          "cust_id",
          "POINT_IN_TIME"
        FROM (
          SELECT
            "cust_id" AS "cust_id",
            "POINT_IN_TIME" AS "POINT_IN_TIME"
          FROM (
            SELECT
              "cust_id" AS "cust_id",
              "POINT_IN_TIME" AS "POINT_IN_TIME"
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
            "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
              "POINT_IN_TIME" IS NOT NULL AND
              "cust_id" IS NOT NULL
        """).strip()
        assert (
            create_table_as_calls[0].kwargs["select_expr"].sql(pretty=True)
            == expected_non_missing_expr
        )

        expected_missing_expr = textwrap.dedent("""
        SELECT
          "cust_id",
          "POINT_IN_TIME"
        FROM (
          SELECT
            "cust_id" AS "cust_id",
            "POINT_IN_TIME" AS "POINT_IN_TIME"
          FROM (
            SELECT
              "cust_id" AS "cust_id",
              "POINT_IN_TIME" AS "POINT_IN_TIME"
            FROM "sf_database"."sf_schema"."sf_table"
          )
        )
        WHERE
            "POINT_IN_TIME" < CAST('2011-03-06T15:37:00' AS TIMESTAMP) AND
            (
                "POINT_IN_TIME" IS NULL OR
                "cust_id" IS NULL
            )
        """).strip()
        assert (
            create_table_as_calls[1].kwargs["select_expr"].sql(pretty=True) == expected_missing_expr
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
            "type": "source_table",
            "feature_store_name": "sf_featurestore",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
            "description": None,
            "target_name": None,
            "treatment_name": None,
        }

    def test_get_purpose(self, test_api_client_persistent, create_success_response):
        """Test get purpose"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response_dict["purpose"] == "other"
        assert response_dict["is_valid"] is True

    def test_update_context(self, test_api_client_persistent, create_success_response):
        """Test update context"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]

        context_id = str(ObjectId())
        context_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/context.json"
        )
        context_payload["_id"] = context_id
        context_payload["name"] = "test_context"
        response = test_api_client.post("/context", json=context_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"context_id": context_id}
        )
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["context_id"] == context_id

    @pytest.mark.asyncio
    async def test_update_use_case(
        self, test_api_client_persistent, create_success_response, create_observation_table
    ):
        """Test update use case"""
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response

        use_case_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/use_case.json"
        )
        new_ob_table_id = ObjectId()
        await create_observation_table(
            new_ob_table_id,
            context_id=use_case_payload["context_id"],
            target_input=True,
            target_id=use_case_payload["target_id"],
        )

        doc_id = str(new_ob_table_id)
        use_case_id = str(ObjectId())
        use_case_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/use_case.json"
        )
        use_case_payload["_id"] = use_case_id
        use_case_payload["name"] = "test_use_case"
        response = test_api_client.post("/use_case", json=use_case_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # test add use case
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert use_case_id in response.json()["use_case_ids"]

        # test add use case duplicate
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot add UseCase {use_case_id} as it is already associated with the ObservationTable."
        )

        # test remove use case
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_remove": use_case_id}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["use_case_ids"] == []

    @pytest.mark.asyncio
    async def test_update_purpose(self, test_api_client_persistent, create_success_response):
        """Test update purpose"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"purpose": "validation_test"}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["purpose"] == "validation_test"

    @pytest.mark.asyncio
    async def test_update_name(self, test_api_client_persistent, create_success_response):
        """Test update name"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"name": "some other name"}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["name"] == "some other name"

    def test_update_use_case_with_error(self, test_api_client_persistent):
        """Test update context route"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        payload["context_id"] = "646f6c1c0ed28a5271fb02d5"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        doc_id = response_dict["payload"]["output_document_id"]

        context_id = str(ObjectId())
        context_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/context.json"
        )
        context_payload["_id"] = context_id
        context_payload["name"] = "test_context"
        response = test_api_client.post("/context", json=context_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        use_case_id = str(ObjectId())
        use_case_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/use_case.json"
        )
        use_case_payload["_id"] = use_case_id
        use_case_payload["name"] = "test_use_case"
        use_case_payload["context_id"] = context_id
        response = test_api_client.post("/use_case", json=use_case_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # test add use_case that has different context
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot add UseCase {use_case_id} due to mismatched contexts."
        )

        # test non_existent use_case_id to add
        non_exist_use_case_id = str(ObjectId())
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_add": non_exist_use_case_id}
        )
        assert response.status_code == HTTPStatus.NOT_FOUND

        # test use_case_id to remove not already associated with the observation table
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_remove": use_case_id}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot remove UseCase {use_case_id} as it is not associated with the ObservationTable."
        )

        # test add use_case to an observation table that is not associated with a context
        observation_table_id = str(ObjectId())
        observation_table_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/observation_table.json"
        )
        observation_table_payload["_id"] = observation_table_id
        observation_table_payload["name"] = "test_observation_table_1"
        observation_table_payload["context_id"] = None
        response = test_api_client.post("/observation_table", json=observation_table_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        response = test_api_client.patch(
            f"{self.base_route}/{observation_table_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot add/remove UseCase as the ObservationTable {observation_table_id} is not associated with any Context."
        )

    @pytest.mark.parametrize(
        "file_type, bad_file",
        [
            ("csv", False),
            ("parquet", False),
            ("parquet", True),
        ],
    )
    def test_upload_observation(self, test_api_client_persistent, file_type, bad_file):
        """
        Test upload route
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # Prepare upload request
        upload_request = ObservationTableUpload(
            name="uploaded_observation_table",
            purpose="other",
            primary_entity_ids=["63f94ed6ea1f050131379214"],
        )
        df = pd.DataFrame({
            "POINT_IN_TIME": ["2023-01-15 10:00:00"],
            "cust_id": ["C1"],
        })
        with tempfile.NamedTemporaryFile(mode="wb", suffix=f".{file_type}") as write_file_obj:
            uploaded_file_name = os.path.basename(write_file_obj.name)
            if file_type == "csv":
                df.to_csv(write_file_obj, index=False)
            elif file_type == "parquet":
                df.to_parquet(write_file_obj, index=False)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            if bad_file:
                write_file_obj.write(b",,,,this_is_bad````")
            write_file_obj.flush()
            with open(write_file_obj.name, "rb") as file_obj:
                files = {"observation_set": file_obj}
                data = {"payload": upload_request.model_dump_json()}

                # Call upload route
                response = test_api_client.post(f"{self.base_route}/upload", data=data, files=files)

        if bad_file:
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
            response_dict = response.json()
            assert response_dict["detail"] == "Content of uploaded file is not valid"
            return

        assert response.status_code == HTTPStatus.CREATED, response.json()
        response_dict = response.json()
        observation_table_id = response_dict["payload"]["output_document_id"]

        # Get observation table
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict

        # Assert response
        assert response_dict["name"] == "uploaded_observation_table"
        assert response_dict["request_input"] == {
            "type": "uploaded_file",
            "file_name": uploaded_file_name,
        }
        assert response_dict["purpose"] == "other"
        expected_columns = {"POINT_IN_TIME", "cust_id"}
        actual_columns = {column["name"] for column in response_dict["columns_info"]}
        assert expected_columns == actual_columns

    def test_create_without_primary_entity_ids_200(self, test_api_client_persistent):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        response = self.post(test_api_client, payload)
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

    @pytest.mark.asyncio
    async def test_delete_target(self, test_api_client_persistent, create_observation_table):
        """Test delete target associated with observation table"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        use_case_payload = self.load_payload("tests/fixtures/request_payloads/use_case.json")
        observation_table_id = ObjectId()
        await create_observation_table(
            observation_table_id,
            context_id=use_case_payload["context_id"],
            target_input=True,
            target_id=use_case_payload["target_id"],
        )

        response = test_api_client.delete(f"use_case/{use_case_payload['_id']}")
        assert response.status_code == HTTPStatus.OK, response.json()

        response = test_api_client.delete(f"target/{use_case_payload['target_id']}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Target is referenced by ObservationTable: new_observation_table"
        )

    def test_upload_observation_validates_columns(self, test_api_client_persistent):
        """
        Test upload route
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # Prepare upload request
        upload_request = ObservationTableUpload(
            name="uploaded_observation_table",
            purpose="other",
            primary_entity_ids=["63f94ed6ea1f050131379214"],
        )
        df = pd.DataFrame({
            "timestamp": ["2023-01-15 10:00:00"],
            "CUST_ID": ["C1"],
        })
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".csv") as write_file_obj:
            df.to_csv(write_file_obj, index=False)
            write_file_obj.flush()
            with open(write_file_obj.name, "rb") as file_obj:
                files = {"observation_set": file_obj}
                data = {"payload": upload_request.model_dump_json()}

                # Call upload route
                response = test_api_client.post(f"{self.base_route}/upload", data=data, files=files)
                assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
                assert (
                    response.json()["detail"]
                    == "Required column(s) not found: POINT_IN_TIME, cust_id"
                )

    @pytest.mark.asyncio
    async def test_create_with_target_column_no_target_422(self, test_api_client_persistent):
        """Test create with target column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "target"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == "Target name not found: target"

    @pytest.mark.asyncio
    async def test_create_with_target_column_missing_column_422(self, test_api_client_persistent):
        """Test create with target column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create target namespace
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/target_namespace.json"
        )
        payload["name"] = "other_target"
        payload["default_target_id"] = None
        payload["target_ids"] = []
        response = test_api_client.post("/target_namespace", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "other_target"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == "Required column(s) not found: other_target"

    @pytest.mark.asyncio
    async def test_create_with_target_column_primary_entity_mismatch_422(
        self, test_api_client_persistent
    ):
        """Test create with target column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create target namespace
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/target_namespace.json"
        )
        payload["name"] = "target"
        payload["entity_ids"] = [str(ObjectId())]
        response = test_api_client.post("/target_namespace", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "target"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert (
            response_dict["detail"] == 'Target "target" does not have matching primary entity ids.'
        )

    def check_target_namespace_classification_metadata_update_task(
        self, test_api_client, observation_table_id, target_namespace_id
    ):
        """Check if the task to update classification metadata is created"""
        target_namespace = test_api_client.get(f"/target_namespace/{target_namespace_id}").json()
        assert target_namespace["target_type"] is None

        # try to update classification metadata
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}/classification_metadata",
            json={"observation_table_id": observation_table_id},
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            f"Target namespace [{target_namespace['name']}] has not been set to classification type."
        )

        # set target type to classification
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}", json={"target_type": "classification"}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["target_type"] == "classification"

        # try to update classification metadata again
        with patch(
            "featurebyte.service.target_namespace.TargetNamespaceService._get_unique_target_values"
        ) as mock_get_unique_target_values:
            mock_get_unique_target_values.return_value = ["true", "false"]
            response = test_api_client.patch(
                f"/target_namespace/{target_namespace_id}/classification_metadata",
                json={"observation_table_id": observation_table_id},
            )
            assert response.status_code == HTTPStatus.ACCEPTED, response.json()

        # check target namespace
        target_namespace = test_api_client.get(f"/target_namespace/{target_namespace_id}").json()
        assert target_namespace["positive_label_candidates"] == [
            {
                "observation_table_id": observation_table_id,
                "positive_label_candidates": ["true", "false"],
            },
        ]

        # test update positive label
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}",
            json={
                "positive_label": {
                    "observation_table_id": observation_table_id,
                    "value": "true",
                }
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["positive_label"] == "true"

        # test updating positive label again (immutability check)
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}",
            json={
                "positive_label": {
                    "observation_table_id": observation_table_id,
                    "value": "false",
                }
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            "Positive label is immutable and cannot be updated once set. "
            "Current positive label value: true."
        )

        # create another observation table without target column
        payload = copy.deepcopy(self.payload)
        new_observation_table_id = str(ObjectId())
        payload["_id"] = new_observation_table_id
        payload["name"] = "observation_table_without_target"
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # try to update classification metadata again with another observation table
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}/classification_metadata",
            json={"observation_table_id": new_observation_table_id},
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            "Observation table [observation_table_without_target] does not associate with any target namespace."
        )

        # try to update target namespace classification metadata with non associated observation table
        # Since positive_label is already set, this should fail with immutability error
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}",
            json={
                "positive_label": {
                    "observation_table_id": new_observation_table_id,
                    "value": "true",
                }
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            "Positive label is immutable and cannot be updated once set. "
            "Current positive label value: true."
        )

    @pytest.mark.asyncio
    @patch(
        "featurebyte.models.observation_table.SourceTableObservationInput.get_column_names_and_dtypes"
    )
    async def test_create_with_target_column_primary_entity_ids_multiple_201(
        self, patch_get_column_names_and_dtypes, test_api_client_persistent
    ):
        """Test create with target column and primary entity ids with multiple entities"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create target namespace
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/target_namespace.json"
        )
        # test with multiple primary entity ids
        payload["name"] = "target"
        payload["dtype"] = "VARCHAR"
        entity_ids = [
            BaseMaterializedTableTestSuite.load_payload(
                "tests/fixtures/request_payloads/entity.json"
            )["_id"],
            BaseMaterializedTableTestSuite.load_payload(
                "tests/fixtures/request_payloads/entity_transaction.json"
            )["_id"],
        ]
        payload["entity_ids"] = entity_ids
        payload["default_target_id"] = None
        payload["target_ids"] = []
        payload["target_type"] = None
        response = test_api_client.post("/target_namespace", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        target_namespace_id = response.json()["_id"]

        patch_get_column_names_and_dtypes.return_value = {
            "cust_id": "INT",
            "POINT_IN_TIME": "TIMESTAMP",
            "target": "UNKNOWN",
            "transaction_id": "INT",
        }
        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "target"
        payload["context_id"] = None
        # reverse order to check order does not matter
        payload["primary_entity_ids"] = entity_ids[::-1]
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        self.check_target_namespace_classification_metadata_update_task(
            test_api_client,
            observation_table_id=self.payload["_id"],
            target_namespace_id=target_namespace_id,
        )

    @pytest.mark.asyncio
    async def test_create_with_target_column_definition_exists_422(
        self, test_api_client_persistent
    ):
        """Test create with target column that has a definition"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "float_target"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == 'Target "float_target" already has a definition.'

    @pytest.mark.asyncio
    async def test_create_with_target_column_201(self, test_api_client_persistent):
        """Test create with target column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create target namespace
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/target_namespace.json"
        )
        payload["name"] = "target"
        payload["default_target_id"] = None
        payload["target_ids"] = []
        response = test_api_client.post("/target_namespace", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        target_namespace_id = response_dict["_id"]
        assert response_dict["target_type"] == "regression"

        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "target"
        payload["request_input"]["source"]["table_details"]["table_name"] = "sf_table_with_target"
        response = self.post(test_api_client, payload)
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        # Get observation table
        response = test_api_client.get(
            f"{self.base_route}/{response_dict['payload']['output_document_id']}"
        )
        assert response.status_code == HTTPStatus.OK, response_dict
        response_dict = response.json()
        observation_table_id = response_dict["_id"]
        assert response_dict["target_namespace_id"] == target_namespace_id

        # attempt to update classification metadata
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}/classification_metadata",
            json={"observation_table_id": observation_table_id},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Target namespace [target] is not of classification type, it is regression type."
        )

        # attempt to set positive label
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}",
            json={
                "positive_label": {
                    "observation_table_id": None,
                    "value": "1.0",
                }
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            "Positive label can only be set for target namespace of type classification, but got regression."
        )

        # test delete target namespace
        response = test_api_client.delete(f"/target_namespace/{target_namespace_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "TargetNamespace is referenced by ObservationTable: observation_table"
        )

    @pytest.mark.asyncio
    async def test_update_use_case_without_target(self, test_api_client_persistent):
        """Test update use case"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        payload["context_id"] = "646f6c1c0ed28a5271fb02d5"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        doc_id = response_dict["payload"]["output_document_id"]

        use_case_id = str(ObjectId())
        use_case_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/use_case.json"
        )
        use_case_payload["_id"] = use_case_id
        use_case_payload["name"] = "test_use_case"
        response = test_api_client.post("/use_case", json=use_case_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # test add use case
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert use_case_id in response.json()["use_case_ids"]

    @pytest.mark.asyncio
    async def test_create_with_use_case_mismatch_422(self, test_api_client_persistent):
        """Test create with target column that does not match use case"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create target namespace
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/target_namespace.json"
        )
        payload["name"] = "target"
        payload["default_target_id"] = None
        payload["target_ids"] = []
        response = test_api_client.post("/target_namespace", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        assert response_dict["target_type"] == "regression"

        payload = copy.deepcopy(self.payload)
        payload["target_column"] = "target"
        payload["primary_entity_ids"] = None
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        payload["request_input"]["source"]["table_details"]["table_name"] = "sf_table_with_target"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert (
            response_dict["detail"]
            == 'Target "target" does not match use case target "float_target".'
        )

    @pytest.mark.asyncio
    async def test_create_with_context_primary_entities_mismatch_422(
        self, test_api_client_persistent
    ):
        """Test create with primary entities and context"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        context_id = str(ObjectId())
        context_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/context.json"
        )
        context_payload["primary_entity_ids"] = ["63f94ed6ea1f050131379204"]
        context_payload["_id"] = context_id
        context_payload["name"] = "test_context"
        response = test_api_client.post("/context", json=context_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        payload = copy.deepcopy(self.payload)
        payload["context_id"] = context_id
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert (
            response_dict["detail"]
            == "Specified primary entity (customer) does not match context primary entity (transaction)."
        )

    @pytest.mark.asyncio
    async def test_create_with_use_case_primary_entities_mismatch_422(
        self, test_api_client_persistent
    ):
        """Test create with primary_entities and use case"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        payload["primary_entity_ids"] = ["63f94ed6ea1f050131379204"]

        with patch(
            "featurebyte.service.observation_table.ObservationTableService._validate_columns"
        ) as mock__validate_columns:
            mock__validate_columns.return_value = ObjectId(), None
            response = self.post(test_api_client, payload)

        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert (
            response_dict["detail"]
            == "Specified primary entity (transaction) does not match context primary entity (customer)."
        )

    @pytest.mark.asyncio
    async def test_create_with_context_use_case_422(self, test_api_client_persistent):
        """Test create with context and use case"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        payload["context_id"] = str(ObjectId())
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert (
            response_dict["detail"] == "Context should not be specified if use case is specified."
        )

    @pytest.mark.asyncio
    @patch("featurebyte.session.base.BaseSession.generate_session_unique_id")
    async def test_create_with_use_case_201(
        self,
        mock_generate_session_unique_id,
        mocked_get_session,
        snowflake_execute_query_for_materialized_table,
        test_api_client_persistent,
        update_fixtures,
    ):
        """Test create eda obs table with use case specified"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        mock_generate_session_unique_id.return_value = "68DFBD342ECAABD7D21D4759"

        # get use case
        response = test_api_client.get("/use_case/64dc9461ad86dba795606745")
        assert response.status_code == HTTPStatus.OK, response.json()
        use_case_target_namespace_id = response.json()["target_namespace_id"]

        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        payload["context_id"] = None
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        payload["purpose"] = "eda"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        observation_table_id = response_dict["payload"]["output_document_id"]
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["context_id"] == "646f6c1c0ed28a5271fb02d5"
        assert response_dict["use_case_ids"] == ["64dc9461ad86dba795606745"]
        assert response_dict["purpose"] == "eda"
        # expect target from use case to be added to observation table
        assert response_dict["target_namespace_id"] == use_case_target_namespace_id

        # check that context default eda table is set
        response = test_api_client.get("/context/646f6c1c0ed28a5271fb02d5")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["default_eda_table_id"] == observation_table_id
        assert response_dict["default_preview_table_id"] is None

        # check that use case default eda table is set
        response = test_api_client.get("/use_case/64dc9461ad86dba795606745")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["default_eda_table_id"] == observation_table_id
        assert response_dict["default_preview_table_id"] is None

        mocked_get_session.execute_query = snowflake_execute_query_for_materialized_table
        queries = extract_session_executed_queries(mocked_get_session, func="execute_query")
        fixture_filename = "tests/fixtures/expected_observation_table_with_use_case.sql"
        assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)

    @pytest.mark.asyncio
    async def test_create_with_use_case_preview_201(self, test_api_client_persistent):
        """Test create preview obs table with use case specified"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        payload["context_id"] = None
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        payload["purpose"] = "preview"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        observation_table_id = response_dict["payload"]["output_document_id"]
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["context_id"] == "646f6c1c0ed28a5271fb02d5"
        assert response_dict["use_case_ids"] == ["64dc9461ad86dba795606745"]
        assert response_dict["purpose"] == "preview"

        # check that context default preview table is set
        response = test_api_client.get("/context/646f6c1c0ed28a5271fb02d5")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["default_eda_table_id"] is None
        assert response_dict["default_preview_table_id"] == observation_table_id

        # check that use case default preview table is set
        response = test_api_client.get("/use_case/64dc9461ad86dba795606745")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["default_eda_table_id"] is None
        assert response_dict["default_preview_table_id"] == observation_table_id

    def test_upload_with_use_case_201(self, test_api_client_persistent):
        """
        Test upload route with use case specified
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # Prepare upload request
        upload_request = ObservationTableUpload(
            name="uploaded_observation_table",
            purpose="other",
            primary_entity_ids=None,
            use_case_id="64dc9461ad86dba795606745",
        )
        df = pd.DataFrame({
            "POINT_IN_TIME": ["2023-01-15 10:00:00"],
            "cust_id": ["C1"],
        })
        with tempfile.NamedTemporaryFile(mode="wb", suffix=".parquet") as write_file_obj:
            uploaded_file_name = os.path.basename(write_file_obj.name)
            df.to_parquet(write_file_obj, index=False)
            write_file_obj.flush()
            with open(write_file_obj.name, "rb") as file_obj:
                files = {"observation_set": file_obj}
                data = {"payload": upload_request.model_dump_json()}

                # Call upload route
                response = test_api_client.post(f"{self.base_route}/upload", data=data, files=files)

        assert response.status_code == HTTPStatus.CREATED, response.json()
        response_dict = response.json()
        observation_table_id = response_dict["payload"]["output_document_id"]

        # Get observation table
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["context_id"] == "646f6c1c0ed28a5271fb02d5"
        assert response_dict["use_case_ids"] == ["64dc9461ad86dba795606745"]

    @pytest.mark.asyncio
    async def test_create_from_obs_table(self, test_api_client_persistent, create_success_response):
        """Test create with an observation table"""
        test_api_client, _ = test_api_client_persistent
        source_observation_table = create_success_response.json()

        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        payload["purpose"] = "preview"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()

        assert response_dict["request_input"] == {
            "observation_table_id": source_observation_table["_id"],
            "downsampling_info": None,
            "type": "source_observation_table",
        }
        assert (
            response_dict["target_namespace_id"] == source_observation_table["target_namespace_id"]
        )
        assert response_dict["primary_entity_ids"] == source_observation_table["primary_entity_ids"]
        assert response_dict["context_id"] == source_observation_table["context_id"]
        assert response_dict["use_case_ids"] == source_observation_table["use_case_ids"]
        assert response_dict["purpose"] == "preview"
        assert response_dict["columns_info"] == source_observation_table["columns_info"]

    @pytest.mark.asyncio
    async def test_create_from_managed_view(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create with an observation table"""
        test_api_client, _ = test_api_client_persistent

        payload = self.load_payload("tests/fixtures/request_payloads/managed_view.json")
        response = test_api_client.post("/managed_view", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        managed_view_id = response_dict["_id"]

        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_managed_view.json"
        )
        payload["use_case_id"] = "64dc9461ad86dba795606745"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()

        assert response_dict["request_input"] == {
            "managed_view_id": managed_view_id,
            "type": "managed_view",
            "columns": None,
            "columns_rename_mapping": None,
        }
        assert response_dict["context_id"] == "646f6c1c0ed28a5271fb02d5"
        assert response_dict["use_case_ids"] == ["64dc9461ad86dba795606745"]
        assert response_dict["purpose"] == "other"

    @pytest.mark.asyncio
    async def test_create_with_downsampling_no_target_422(
        self, test_api_client_persistent, create_success_response, update_fixtures
    ):
        """Test create with an observation table and downsampling without target column"""
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["request_input"]["downsampling_info"] = DownSamplingInfo(
            sampling_rate_per_target_value=[
                {"target_value": "1", "rate": 0.5},
            ]
        ).model_dump(by_alias=False)
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_dict = response.json()
        assert response_dict["detail"] == (
            "Downsampling by target value requires the source observation table to have a target column."
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "rate,type,msg,cond,threshold",
        [
            (-0.5, "greater_than", "Input should be greater than 0", "gt", 0),
            (0, "greater_than", "Input should be greater than 0", "gt", 0),
            (1.01, "less_than_equal", "Input should be less than or equal to 1", "le", 1),
        ],
    )
    async def test_create_with_downsampling_invalid_rate_422(
        self, test_api_client_persistent, create_success_response, rate, type, msg, cond, threshold
    ):
        """Test create with an observation table and invalid downsampling rate"""
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["request_input"]["downsampling_info"] = {
            "sampling_rate_per_target_value": [
                {"target_value": "1", "rate": rate},
            ]
        }
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_dict = response.json()
        assert response_dict["detail"] == [
            {
                "type": type,
                "loc": [
                    "body",
                    "request_input",
                    "source_observation_table",
                    "downsampling_info",
                    "sampling_rate_per_target_value",
                    0,
                    "rate",
                ],
                "msg": msg,
                "input": rate,
                "ctx": {cond: threshold},
            }
        ]

    @pytest.mark.asyncio
    async def test_create_with_downsampling_no_downsampling_422(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create with an observation table and downsampling rate that does not require downsampling"""
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["request_input"]["downsampling_info"] = {
            "sampling_rate_per_target_value": [
                {"target_value": "1", "rate": 1.0},
            ],
        }
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_dict = response.json()
        assert response_dict["detail"] == [
            {
                "type": "value_error",
                "loc": [
                    "body",
                    "request_input",
                    "source_observation_table",
                    "downsampling_info",
                ],
                "msg": "Value error, At least one sampling rate must be less than 1.0 to enable downsampling",
                "input": {"sampling_rate_per_target_value": [{"target_value": "1", "rate": 1.0}]},
                "ctx": {"error": {}},
            }
        ]

    @pytest.mark.asyncio
    async def test_create_with_downsampling_duplicate_target_values_422(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create with an observation table and multiple downsampling rate for same target value"""
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["request_input"]["downsampling_info"] = {
            "sampling_rate_per_target_value": [
                {"target_value": "1", "rate": 0.5},
                {"target_value": "1", "rate": 0.1},
            ],
        }
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_dict = response.json()
        assert response_dict["detail"] == [
            {
                "type": "value_error",
                "loc": [
                    "body",
                    "request_input",
                    "source_observation_table",
                    "downsampling_info",
                    "sampling_rate_per_target_value",
                ],
                "msg": "Value error, Duplicate target value found: 1",
                "input": [{"target_value": "1", "rate": 0.5}, {"target_value": "1", "rate": 0.1}],
                "ctx": {"error": {}},
            }
        ]

    @pytest.mark.asyncio
    @patch("featurebyte.session.base.BaseSession.generate_session_unique_id")
    async def test_create_with_downsampling_201(
        self,
        mock_generate_session_unique_id,
        mocked_get_session,
        snowflake_execute_query_for_materialized_table,
        test_api_client_persistent,
        update_fixtures,
    ):
        """Test create with an observation table and downsampling"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        mock_generate_session_unique_id.return_value = "68DFBD342ECAABD7D21D4759"

        # create boolean target namespace
        payload = self.load_payload("tests/fixtures/request_payloads/target_namespace.json")
        payload["dtype"] = "BOOL"
        payload["name"] = "bool_target"
        payload["target_type"] = None
        payload["target_ids"] = []
        response = test_api_client.post("/target_namespace", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        target_namespace_id = response.json()["_id"]

        payload = copy.deepcopy(self.payload)
        payload["primary_entity_ids"] = None
        payload["context_id"] = None
        payload["purpose"] = "eda"
        payload["target_column"] = "bool_target"
        payload["request_input"]["source"]["table_details"]["table_name"] = (
            "sf_table_with_target_namespace"
        )
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        task_response = self.wait_for_results(test_api_client, response)
        task_response_dict = task_response.json()
        assert task_response_dict["status"] == "SUCCESS", task_response_dict["traceback"]

        observation_table_id = response_dict["payload"]["output_document_id"]
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        assert response.status_code == HTTPStatus.OK, response_dict
        source_observation_table_id = response.json()["_id"]

        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["use_case_id"] = None
        payload["request_input"]["observation_table_id"] = source_observation_table_id
        payload["request_input"]["downsampling_info"] = {
            "sampling_rate_per_target_value": [
                {"target_value": True, "rate": 0.5},
            ],
            "default_sampling_rate": 0.2,
        }

        # expect failure due to target type not set
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert (
            response_dict["detail"]
            == "Downsampling by target value is only supported for classification targets."
        )

        # update target type
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}",
            json={"target_type": "classification"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()

        assert response_dict["request_input"] == {
            "observation_table_id": source_observation_table_id,
            "downsampling_info": payload["request_input"]["downsampling_info"],
            "type": "source_observation_table",
        }
        assert response_dict["has_row_weights"] is True

        # downsample from an already downsampled observation table
        new_source_observation_table_id = response_dict["_id"]
        payload["_id"] = str(ObjectId())
        payload["name"] = "Double downsampled observation table"
        payload["request_input"]["observation_table_id"] = new_source_observation_table_id
        payload["request_input"]["downsampling_info"] = {
            "sampling_rate_per_target_value": [
                {"target_value": True, "rate": 0.3},
            ],
            "default_sampling_rate": 1.0,
        }
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()
        assert response_dict["request_input"] == {
            "observation_table_id": new_source_observation_table_id,
            "downsampling_info": payload["request_input"]["downsampling_info"],
            "type": "source_observation_table",
        }
        assert response_dict["has_row_weights"] is True

        # sample from an already downsampled observation table without further downsampling
        payload["_id"] = str(ObjectId())
        payload["name"] = "Sampled from downsampled observation table"
        payload["request_input"]["observation_table_id"] = new_source_observation_table_id
        payload["request_input"]["downsampling_info"] = None
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()
        assert response_dict["request_input"] == {
            "observation_table_id": new_source_observation_table_id,
            "downsampling_info": None,
            "type": "source_observation_table",
        }
        # ensure that has_row_weights is still True
        assert response_dict["has_row_weights"] is True

        mocked_get_session.execute_query = snowflake_execute_query_for_materialized_table
        queries = extract_session_executed_queries(mocked_get_session, func="execute_query")
        fixture_filename = "tests/fixtures/expected_observation_table_with_downsampling.sql"
        assert_equal_with_expected_fixture(queries, fixture_filename, update_fixtures)

    @pytest.mark.asyncio
    async def test_create_with_downsampling_and_sample_rows_422(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create with an observation table and apply both downsampling rate and sample rows"""
        test_api_client, _ = test_api_client_persistent
        payload = self.load_payload(
            "tests/fixtures/request_payloads/observation_table_from_obs_table.json"
        )
        payload["request_input"]["downsampling_info"] = {
            "sampling_rate_per_target_value": [
                {"target_value": "1", "rate": 0.5},
            ],
        }
        payload["sample_rows"] = 10
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_dict = response.json()
        assert (
            response_dict["detail"]
            == "Downsampling by both target value and sample rows is not supported."
        )

    @pytest.mark.asyncio
    async def test_create_with_treatment_column_no_treatment_422(self, test_api_client_persistent):
        """Test create with treatment column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = copy.deepcopy(self.payload)
        payload["treatment_column"] = "treatment"
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == "Treatment name not found: treatment"

    @pytest.mark.asyncio
    async def test_create_with_treatment_column_missing_column_422(
        self, test_api_client_persistent
    ):
        """Test create with treatment column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        treatment_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/treatment.json"
        )
        treatment_name = treatment_payload["name"]

        payload = copy.deepcopy(self.payload)
        payload["treatment_column"] = treatment_name
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == f"Required column(s) not found: {treatment_name}"

    @pytest.mark.asyncio
    @patch(
        "featurebyte.models.observation_table.SourceTableObservationInput.get_column_names_and_dtypes"
    )
    async def test_treatment_labels_invalid_task(
        self, patch_get_column_names_and_dtypes, test_api_client_persistent
    ):
        """Test check if treatment labels is valid"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        treatment_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/treatment.json"
        )
        treatment_id = treatment_payload["_id"]

        patch_get_column_names_and_dtypes.return_value = {
            "cust_id": "INT",
            "POINT_IN_TIME": "TIMESTAMP",
            treatment_payload["name"]: "INT",
            "transaction_id": "INT",
        }
        payload = copy.deepcopy(self.payload)
        payload["treatment_column"] = treatment_payload["name"]
        payload["context_id"] = None
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        observation_table_id = payload["_id"]

        response = test_api_client.patch(
            f"/treatment/{treatment_id}/treatment_labels",
            json={"observation_table_id": observation_table_id},
        )
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "FAILURE"
        assert (
            "Treatment labels [0, 1] are different from treatment values []. "
            "Ensure the treatment labels match the observation table values."
        ) in response_dict["traceback"]

    @pytest.mark.asyncio
    async def test_create_with_wrong_treatment_values(self, test_api_client_persistent):
        """Test create with wrong treatment values"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create treatment
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/treatment.json"
        )
        payload["name"] = "treatment"
        payload["_id"] = str(ObjectId("693a5511712c9d05fb3203da"))
        response = test_api_client.post("/treatment", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        treatment_id = response_dict["_id"]
        assert response_dict["treatment_type"] == "binary"

        with patch(
            "featurebyte.service.treatment.TreatmentService._get_unique_treatment_values"
        ) as mock_get_unique_treatment_values:
            mock_get_unique_treatment_values.return_value = [False, True]
            payload = copy.deepcopy(self.payload)
            payload["treatment_column"] = "treatment"
            payload["request_input"]["source"]["table_details"]["table_name"] = (
                "sf_table_with_treatment"
            )
            response = self.post(test_api_client, payload)
            response = self.wait_for_results(test_api_client, response)
            response_dict = response.json()
            assert response_dict["status"] == "FAILURE"
            assert (
                "Treatment labels [0, 1] are different from treatment values [False, True]. "
                "Ensure the treatment labels match the observation table values."
            ) in response_dict["traceback"]

    @pytest.mark.asyncio
    async def test_create_with_treatment_column_201(self, test_api_client_persistent):
        """Test create with treatment column"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create treatment
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/treatment.json"
        )
        payload["name"] = "treatment"
        payload["_id"] = str(ObjectId("693a5511712c9d05fb3203da"))
        response = test_api_client.post("/treatment", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        treatment_id = response_dict["_id"]
        assert response_dict["treatment_type"] == "binary"

        with patch(
            "featurebyte.service.treatment.TreatmentService._get_unique_treatment_values"
        ) as mock_get_unique_treatment_values:
            mock_get_unique_treatment_values.return_value = [0, 1]
            payload = copy.deepcopy(self.payload)
            payload["treatment_column"] = "treatment"
            payload["request_input"]["source"]["table_details"]["table_name"] = (
                "sf_table_with_treatment"
            )
            response = self.post(test_api_client, payload)
            response = self.wait_for_results(test_api_client, response)
            response_dict = response.json()
            assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        # Get observation table
        response = test_api_client.get(
            f"{self.base_route}/{response_dict['payload']['output_document_id']}"
        )
        assert response.status_code == HTTPStatus.OK, response_dict
        response_dict = response.json()
        observation_table_id = response_dict["_id"]
        assert response_dict["treatment_id"] == treatment_id

        # test delete treatment
        response = test_api_client.delete(f"/treatment/{treatment_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Treatment is referenced by ObservationTable: observation_table"
        )

    @pytest.mark.asyncio
    async def test_create_with_treatment_context_mismatch_422(self, test_api_client_persistent):
        """Test create with treatment column that does not match context"""

        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create treatment
        payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/treatment.json"
        )
        payload["name"] = "treatment"
        payload["_id"] = str(ObjectId("693a5511712c9d05fb3203da"))
        response = test_api_client.post("/treatment", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        treatment_id = response_dict["_id"]
        assert response_dict["treatment_type"] == "binary"

        context_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/context_with_treatment.json"
        )

        with patch(
            "featurebyte.service.treatment.TreatmentService._get_unique_treatment_values"
        ) as mock_get_unique_treatment_values:
            mock_get_unique_treatment_values.return_value = [0, 1]
            payload = copy.deepcopy(self.payload)
            payload["treatment_column"] = "treatment"
            payload["context_id"] = context_payload["_id"]
            payload["request_input"]["source"]["table_details"]["table_name"] = (
                "sf_table_with_treatment"
            )
            response = self.post(test_api_client, payload)
            response_dict = response.json()
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
            assert (
                response_dict["detail"]
                == 'Treatment "treatment" does not match context treatment "test_treatment".'
            )

    @pytest.mark.asyncio
    async def test_create_with_treatment_mapping(self, test_api_client_persistent):
        """Test create with treatment column that requires mapping"""

        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        treatmemt_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/treatment.json"
        )
        context_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/context_with_treatment.json"
        )

        with patch(
            "featurebyte.service.treatment.TreatmentService._get_unique_treatment_values"
        ) as mock_get_unique_treatment_values:
            mock_get_unique_treatment_values.return_value = [0, 1]
            payload = copy.deepcopy(self.payload)
            payload["treatment_column"] = treatmemt_payload["name"]
            payload["request_input"]["columns_rename_mapping"] = {
                "treatment": treatmemt_payload["name"]
            }
            payload["context_id"] = context_payload["_id"]
            payload["request_input"]["source"]["table_details"]["table_name"] = (
                "sf_table_with_treatment"
            )
            response = self.post(test_api_client, payload)
            response = self.wait_for_results(test_api_client, response)
            response_dict = response.json()
            assert response_dict["status"] == "SUCCESS", response_dict["traceback"]
