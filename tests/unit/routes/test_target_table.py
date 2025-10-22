"""
Tests for target table routes
"""

from http import HTTPStatus

import pandas as pd
import pytest
from bson.objectid import ObjectId

from featurebyte.common.utils import dataframe_to_arrow_bytes
from featurebyte.models.observation_table import TargetInput
from featurebyte.schema.target_table import TargetTableCreate
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

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("context", "context"),
            ("observation_table", "observation_table"),
            ("event_table", "event_table"),
            ("target", "target"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)

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
            payload["name"] = f"{self.payload['name']}_{i}"
            yield payload

    @pytest.fixture(autouse=True)
    def always_patched_observation_table_service(self, patched_observation_table_service):
        """
        Patch ObservationTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_observation_table_service

    @property
    def download_filename_prefix(self):
        return "observation_table"

    def test_schema(self, test_api_client_persistent, create_success_response, user_id):
        """Test schema of create success response"""
        assert create_success_response.status_code == HTTPStatus.OK
        json_dict = create_success_response.json()

        # get target namespace
        test_api_client, _ = test_api_client_persistent
        target_id = json_dict["request_input"]["target_id"]
        response = test_api_client.get(f"/target/{target_id}")
        assert response.status_code == HTTPStatus.OK
        target_namespace_id = response.json()["target_namespace_id"]
        catalog_id = json_dict["catalog_id"]

        assert json_dict == {
            "_id": json_dict["_id"],
            "block_modification_by": [],
            "catalog_id": catalog_id,
            "columns_info": [
                {
                    "dtype": "TIMESTAMP",
                    "entity_id": None,
                    "name": "POINT_IN_TIME",
                    "dtype_metadata": None,
                    "partition_metadata": None,
                },
                {
                    "dtype": "INT",
                    "entity_id": None,
                    "name": "cust_id",
                    "dtype_metadata": None,
                    "partition_metadata": None,
                },
            ],
            "context_id": None,
            "created_at": json_dict["created_at"],
            "description": None,
            "entity_column_name_to_count": {},
            "has_row_index": True,
            "is_view": False,
            "least_recent_point_in_time": None,
            "location": json_dict["location"],
            "min_interval_secs_between_entities": None,
            "most_recent_point_in_time": "2023-01-15 10:00:00",
            "name": "target_table",
            "num_rows": 100,
            "primary_entity_ids": ["63f94ed6ea1f050131379214"],
            "purpose": "other",
            "request_input": {
                "observation_table_id": "646f6c1c0ed28a5271fb02d7",
                "target_id": "64a80107d667dd0c2b13d8cd",
                "type": "observation_table",
            },
            "target_namespace_id": target_namespace_id,
            "updated_at": None,
            "use_case_ids": [],
            "user_id": str(user_id),
            "is_deleted": False,
            "sample_from_timestamp": None,
            "sample_rows": None,
            "sample_to_timestamp": None,
            "sampling_rate_per_target_value": None,
            "table_with_missing_data": None,
            "is_valid": True,
        }

    @pytest.mark.parametrize(
        "target_id,observation_table_id,input_type",
        [
            ("64a80107d667dd0c2b13d8cd", "646f6c1c0ed28a5271fb02d7", "observation_table"),
            ("64a80107d667dd0c2b13d8cd", None, "dataframe"),
        ],
    )
    def test_create_201_backward_compatible(
        self, test_api_client_persistent, target_id, observation_table_id, input_type
    ):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        payload = TargetTableCreate(**self.payload).json_dict()

        # get target namespace
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"/target/{target_id}")
        assert response.status_code == HTTPStatus.OK
        target_namespace_id = response.json()["target_namespace_id"]

        # always populate graph and node_names
        target_payload = self.load_payload("tests/fixtures/request_payloads/target.json")
        payload["graph"] = target_payload["graph"]
        payload["node_names"] = [target_payload["node_name"]]

        # no target_id, request_input is TargetInput
        payload["target_id"] = None
        payload["observation_table_id"] = observation_table_id
        payload["request_input"] = TargetInput(
            target_id=target_id,
            observation_table_id=observation_table_id,
            type=input_type,
        ).json_dict()

        if not observation_table_id:
            df = pd.DataFrame({
                "POINT_IN_TIME": ["2023-01-15 10:00:00"],
                "cust_id": ["C1"],
            })
            files = {"observation_set": dataframe_to_arrow_bytes(df)}
            response = self.post(test_api_client, payload, files=files)
        else:
            response = self.post(test_api_client, payload)
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["request_input"] == {
            "target_id": target_id,
            "observation_table_id": observation_table_id,
            "type": input_type,
        }
        assert response_dict["target_namespace_id"] == target_namespace_id
