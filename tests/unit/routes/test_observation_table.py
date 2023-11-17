"""
Tests for ObservationTable routes
"""
import copy
import os
import tempfile
from http import HTTPStatus

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
            ("event_table", "event_table"),
            ("item_table", "item_table"),
            ("context", "context"),
            ("target", "target"),
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
        }

    def test_get_purpose(self, test_api_client_persistent, create_success_response):
        """Test get purpose"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response_dict["purpose"] == "other"

    def test_update_context(self, test_api_client_persistent, create_success_response):
        """Test update context route"""
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
        """Test update context route"""
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

    def test_update_use_case_with_error(self, test_api_client_persistent, create_success_response):
        """Test update context route"""
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

        use_case_id = str(ObjectId())
        use_case_payload = BaseMaterializedTableTestSuite.load_payload(
            "tests/fixtures/request_payloads/use_case.json"
        )
        use_case_payload["_id"] = use_case_id
        use_case_payload["name"] = "test_use_case"
        use_case_payload["context_id"] = context_id
        response = test_api_client.post("/use_case", json=use_case_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # test update use_case_ids which is not allowed
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_ids": [use_case_id]}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "use_case_ids is not a valid field to update"

        # test add use_case that has different context
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot add UseCase {use_case_id} as its context_id is different from the existing context_id."
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

    @pytest.mark.parametrize("file_type", ["csv", "parquet"])
    def test_upload_observation(self, test_api_client_persistent, file_type):
        """
        Test upload route
        """
        test_api_client, _ = test_api_client_persistent

        # Prepare upload request
        upload_request = ObservationTableUpload(
            name="uploaded_observation_table",
            purpose="other",
            primary_entity_ids=["63f94ed6ea1f050131379214"],
        )
        df = pd.DataFrame(
            {
                "POINT_IN_TIME": ["2023-01-15 10:00:00"],
                "CUST_ID": ["C1"],
            }
        )
        with tempfile.NamedTemporaryFile(mode="wb", suffix=f".{file_type}") as write_file_obj:
            uploaded_file_name = os.path.basename(write_file_obj.name)
            if file_type == "csv":
                df.to_csv(write_file_obj, index=False)
            elif file_type == "parquet":
                df.to_parquet(write_file_obj, index=False)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
            write_file_obj.flush()
            with open(write_file_obj.name, "rb") as file_obj:
                files = {"observation_set": file_obj}
                data = {"payload": upload_request.json()}

                # Call upload route
                response = test_api_client.post(f"{self.base_route}/upload", data=data, files=files)
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

        response = test_api_client.delete(f"target/{use_case_payload['target_id']}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Target is referenced by ObservationTable: observation_table_from_target_input"
        )
