"""
Tests for Use Case route
"""
from functools import partial
from http import HTTPStatus

import pytest
from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestUseCaseApi(BaseCatalogApiTestSuite):
    """
    TestUseCaseApi class
    """

    class_name = "UseCase"
    base_route = "/use_case"
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/use_case.json")
    unknown_id = ObjectId()
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'UseCase (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `UseCase.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "context_id": "test_id"},
            [
                {
                    "loc": ["body", "context_id"],
                    "msg": "Id must be of type PydanticObjectId",
                    "type": "type_error",
                }
            ],
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

    @pytest.fixture(name="location")
    def location_fixture(self):
        """
        location fixture
        """
        return {
            "feature_store_id": ObjectId(),
            "table_details": {
                "database_name": "fb_database",
                "schema_name": "fb_schema",
                "table_name": "fb_materialized_table",
            },
        }

    @pytest.fixture(name="create_observation_table")
    def create_observation_table_fixture(
        self, test_api_client_persistent, location, default_catalog_id, user_id
    ):
        """
        simulate creating observation table for target input with the same target_id and context_id
        """

        _, persistent = test_api_client_persistent

        async def create_observation_table(ob_table_id):
            request_input = {
                "target_id": ObjectId(self.payload["target_id"]),
                "observation_table_id": ob_table_id,
                "type": "dataframe",
            }
            await persistent.insert_one(
                collection_name="observation_table",
                document={
                    "_id": ob_table_id,
                    "name": "observation_table_from_target_input",
                    "request_input": request_input,
                    "location": location,
                    "columns_info": [
                        {"name": "a", "dtype": "INT"},
                        {"name": "b", "dtype": "INT"},
                        {"name": "c", "dtype": "INT"},
                    ],
                    "num_rows": 1000,
                    "most_recent_point_in_time": "2023-01-15T10:00:00",
                    "context_id": ObjectId(self.payload["context_id"]),
                    "catalog_id": ObjectId(default_catalog_id),
                    "user_id": user_id,
                },
                user_id=user_id,
            )

        return partial(create_observation_table)

    @pytest.mark.asyncio
    async def test_automated_assign_observation_table(
        self,
        test_api_client_persistent,
        create_observation_table,
    ):
        """Test automated assign observation table when creating use case"""

        test_api_client, _ = test_api_client_persistent
        ob_table_id = ObjectId()

        await create_observation_table(ob_table_id)

        response = test_api_client.post(self.base_route, json=self.payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        created_use_case = response.json()
        assert created_use_case["context_id"] == self.payload["context_id"]
        assert created_use_case["target_id"] == self.payload["target_id"]
        assert created_use_case["description"] == self.payload["description"]
        assert created_use_case["observation_table_ids"] == [str(ob_table_id)]

    @pytest.mark.asyncio
    async def test_update_use_case(
        self,
        create_success_response,
        test_api_client_persistent,
        create_observation_table,
    ):
        """Test update use case"""

        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        new_ob_table_id_1 = ObjectId()
        await create_observation_table(new_ob_table_id_1)
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"new_observation_table_id": str(new_ob_table_id_1)},
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["context_id"] == self.payload["context_id"]
        assert data["target_id"] == self.payload["target_id"]
        assert data["description"] == self.payload["description"]
        assert len(data["observation_table_ids"]) == 1
        assert data["observation_table_ids"] == [str(new_ob_table_id_1)]

        new_ob_table_id_2 = ObjectId()
        await create_observation_table(new_ob_table_id_2)

        new_ob_table_id_3 = ObjectId()
        await create_observation_table(new_ob_table_id_3)

        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "default_preview_table_id": str(new_ob_table_id_2),
                "default_eda_table_id": str(new_ob_table_id_3),
            },
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert len(data["observation_table_ids"]) == 3
        assert set(data["observation_table_ids"]) == {
            str(new_ob_table_id_1),
            str(new_ob_table_id_2),
            str(new_ob_table_id_3),
        }
        assert data["default_preview_table_id"] == str(new_ob_table_id_2)
        assert data["default_eda_table_id"] == str(new_ob_table_id_3)

    @pytest.mark.asyncio
    async def test_list_feature_tables(
        self,
        create_success_response,
        test_api_client_persistent,
        user_id,
        default_catalog_id,
        location,
        create_observation_table,
    ):
        """Test list feature tables for use case"""
        test_api_client, persistent = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        new_ob_table_id = ObjectId()
        await create_observation_table(new_ob_table_id)
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"new_observation_table_id": str(new_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["observation_table_ids"] == [str(new_ob_table_id)]

        feature_table_payload = BaseCatalogApiTestSuite.load_payload(
            "tests/fixtures/request_payloads/historical_feature_table.json"
        )
        feature_table_payload["_id"] = ObjectId()
        feature_table_payload["name"] = "random_name"

        await persistent.insert_one(
            collection_name="historical_feature_table",
            document={
                **feature_table_payload,
                "catalog_id": ObjectId(default_catalog_id),
                "user_id": user_id,
                "observation_table_id": new_ob_table_id,
                "columns_info": [],
                "num_rows": 500,
                "location": location,
                "feature_list_id": ObjectId(),
            },
            user_id=user_id,
        )

        response = test_api_client.get(f"{self.base_route}/{use_case_id}/feature_tables")
        assert response.status_code == HTTPStatus.OK, response.json()
        data = response.json()
        assert len(data) == 1
        assert data[0]["_id"] == str(feature_table_payload["_id"])
        assert data[0]["name"] == feature_table_payload["name"]
        assert data[0]["observation_table_id"] == str(new_ob_table_id)
