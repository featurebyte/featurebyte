"""
Tests for ObservationTable routes
"""
from http import HTTPStatus

import pandas as pd
import pytest
from bson.objectid import ObjectId

from featurebyte.common.utils import dataframe_to_arrow_bytes
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

    def test_update_use_case(self, test_api_client_persistent, create_success_response):
        """Test update context route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]

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
        assert response.status_code == HTTPStatus.OK
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

    def test_upload_observation_csv(self, test_api_client_persistent, snowflake_feature_store_id):
        """
        Test put CSV route
        """
        test_api_client, _ = test_api_client_persistent

        # Prepare upload request
        upload_request = ObservationTableUpload(
            name="uploaded_observation_table",
            feature_store_id=snowflake_feature_store_id,
            purpose="other",
        )
        df = pd.DataFrame(
            {
                "POINT_IN_TIME": ["2023-01-15 10:00:00"],
                "CUST_ID": ["C1"],
            }
        )
        files = {"observation_set": dataframe_to_arrow_bytes(df)}
        data = {"payload": upload_request.json()}

        # Call upload route
        response = test_api_client.put(self.base_route, data=data, files=files)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        response_dict = response.json()
        observation_table_id = response_dict["payload"]["output_document_id"]

        # Get observation table
        response = test_api_client.get(f"{self.base_route}/{observation_table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict

        # Assert response
        assert response_dict["name"] == "uploaded_observation_table"
        assert response_dict["request_input"] == {"type": "uploaded_file"}
        assert response_dict["purpose"] == "other"
        expected_columns = {"POINT_IN_TIME", "cust_id"}
        actual_columns = {column["name"] for column in response_dict["columns_info"]}
        assert expected_columns == actual_columns
