"""
Tests for TargetTable routes
"""
import copy
from http import HTTPStatus

import pandas as pd
import pytest
from bson.objectid import ObjectId

from featurebyte.common.utils import dataframe_to_arrow_bytes
from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseMaterializedTableTestSuite


class TestTargetTableApi(BaseMaterializedTableTestSuite):
    """
    Tests for TargetTable route
    """

    wrap_payload_on_create = True

    class_name = "TargetTable"
    base_route = "/target_table"
    payload = BaseMaterializedTableTestSuite.load_payload(
        "tests/fixtures/request_payloads/target_table.json"
    )
    random_id = str(ObjectId())

    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'TargetTable (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `TargetTable.get(name="{payload["name"]}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'TargetTable (name: "{payload["name"]}") already exists. '
            f'Get the existing object by `TargetTable.get(name="{payload["name"]}")`.',
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("context", "context"),
            ("observation_table", "observation_table"),
            ("event_table", "event_table"),
            ("target", "target"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}",
                headers={"active-catalog-id": str(catalog_id)},
                json=payload,
            )

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

    def test_create_422__failed_entity_validation_check(self, test_api_client_persistent):
        """Test that 422 is returned when payload fails validation check"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = copy.deepcopy(self.payload)
        payload["serving_names_mapping"] = {"random_name": "random_name"}

        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Unexpected serving names provided in serving_names_mapping: random_name"
        )

    @pytest.mark.skip(reason="skip for now since the observation table isn't used fully yet.")
    async def test_observation_table_delete_422__observation_table_failed_validation_check(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Test delete 422 for observation table failed validation check"""
        test_api_client, persistent = test_api_client_persistent
        create_success_response_dict = create_success_response.json()
        target_table_id = create_success_response_dict["_id"]

        # insert another document to historical feature table to make sure the query filter is correct
        payload = copy.deepcopy(self.payload)
        payload["_id"] = ObjectId()
        payload["name"] = "random_name"
        await persistent.insert_one(
            collection_name="target_table",
            document={
                **payload,
                "_id": ObjectId(),
                "catalog_id": DEFAULT_CATALOG_ID,
                "user_id": user_id,
                "observation_table_id": ObjectId(),  # different batch request table id
                "columns_info": [],
                "num_rows": 500,
                "location": create_success_response_dict["location"],
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
            f"Cannot delete Observation Table {observation_table_id} because it is referenced by "
            f"1 Target Table(s): ['{target_table_id}']"
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
            "target_name": "float_target",
            "observation_table_name": "observation_table",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": response_dict["table_details"]["table_name"],
            },
            "created_at": response_dict["created_at"],
            "updated_at": None,
        }

    def test_provide_both_observation_table_id_and_dataframe_not_allowed(
        self, test_api_client_persistent
    ):
        """
        Test that providing both observation_table_id and observation set DataFrame is not allowed
        """
        test_api_client, _ = test_api_client_persistent
        df = pd.DataFrame(
            {
                "POINT_IN_TIME": ["2023-01-15 10:00:00"],
                "CUST_ID": ["C1"],
            }
        )
        files = {"observation_set": dataframe_to_arrow_bytes(df)}
        response = self.post(test_api_client, self.payload, files=files)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": "Only one of observation_set file and observation_table_id can be set"
        }
