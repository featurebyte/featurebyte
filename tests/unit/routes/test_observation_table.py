"""
Tests for ObservationTable routes
"""

import copy
import json
import os
import tempfile
import textwrap
from http import HTTPStatus
from unittest.mock import patch

import pandas as pd
import pytest
from bson.objectid import ObjectId

from featurebyte.schema.observation_table import ObservationTableUpload
from tests.unit.routes.base import BaseMaterializedTableTestSuite


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
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

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

    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        with patch("featurebyte.session.base.BaseSession.create_table_as") as mock:
            super().test_create_201__without_specifying_id_field(test_api_client_persistent)

        # check create_table_as expression
        create_table_as_calls = mock.call_args_list
        expected_non_missing_expr = textwrap.dedent("""
        SELECT
          "cust_id",
          "POINT_IN_TIME",
          "target"
        FROM (
          SELECT
            "cust_id" AS "cust_id",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "target" AS "target"
          FROM (
            SELECT
              *
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
          "POINT_IN_TIME",
          "target"
        FROM (
          SELECT
            "cust_id" AS "cust_id",
            "POINT_IN_TIME" AS "POINT_IN_TIME",
            "target" AS "target"
          FROM (
            SELECT
              *
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
            == "Target is referenced by ObservationTable: observation_table_from_target_input"
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

        # check update non-valid positive label
        response = test_api_client.patch(
            f"/target_namespace/{target_namespace_id}",
            json={
                "positive_label": {
                    "observation_table_id": observation_table_id,
                    "value": "invalid_label",
                }
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response_dict
        assert response_dict["detail"] == (
            'Value "invalid_label" is not a valid candidate for observation '
            "table (ID: 646f6c1c0ed28a5271fb02d7). Valid candidates are: ['true', 'false']."
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
            "Please run target namespace classification metadata update task to extract positive label "
            "candidates before setting the positive label."
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
                    "observation_table_id": observation_table_id,
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
            mock__validate_columns.return_value = ObjectId()
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
    async def test_create_with_use_case_201(self, test_api_client_persistent):
        """Test create with use case specified"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

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

        # check that use case default eda table is not set
        response = test_api_client.get("/use_case/64dc9461ad86dba795606745")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["default_eda_table_id"] is None

        # create target table
        target_table_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/target_table.json"
        )
        response = test_api_client.post(
            "/target_table", data={"payload": json.dumps(target_table_payload)}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        observation_table_id = response_dict["payload"]["output_document_id"]
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["context_id"] == "646f6c1c0ed28a5271fb02d5"
        assert response_dict["use_case_ids"] == ["64dc9461ad86dba795606745"]
        assert response_dict["purpose"] == "eda"

        # check that use case default eda table is set
        response = test_api_client.get("/use_case/64dc9461ad86dba795606745")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["default_eda_table_id"] == observation_table_id

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
