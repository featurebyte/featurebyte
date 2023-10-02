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
        simulate creating observation table for target input, target_id and context_id
        """

        _, persistent = test_api_client_persistent

        async def create_observation_table(
            ob_table_id, target_input=True, same_context=True, same_target=True, new_context_id=None
        ):
            context_id = ObjectId(self.payload["context_id"])
            if not same_context:
                context_id = new_context_id

            target_id = ObjectId(self.payload["target_id"])
            if not same_target:
                target_id = ObjectId()

            request_input = {
                "target_id": target_id,
                "observation_table_id": ob_table_id,
                "type": "dataframe",
            }
            if not target_input:
                request_input = {
                    "columns": None,
                    "columns_rename_mapping": None,
                    "source": location,
                    "type": "source_table",
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
                    "context_id": context_id,
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
        create_success_response,
    ):
        """Test automated assign observation table when creating use case"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent
        target_ob_table_id = ObjectId()
        non_target_ob_table_id = ObjectId()
        different_context_ob_table_id = ObjectId()

        # create observation table with target input
        await create_observation_table(target_ob_table_id)
        # create observation table with non target input
        await create_observation_table(non_target_ob_table_id, target_input=False)
        # create observation table with different context
        await create_observation_table(different_context_ob_table_id, same_context=False)

        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["name"] = payload["name"] + "_1"
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        created_use_case = response.json()
        assert created_use_case["context_id"] == self.payload["context_id"]
        assert created_use_case["target_id"] == self.payload["target_id"]
        assert created_use_case["description"] == self.payload["description"]

        response = test_api_client.get(
            f"{self.base_route}/{created_use_case['_id']}/observation_tables",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 1
        assert response.json()["data"][0]["_id"] == str(target_ob_table_id)

    @pytest.mark.asyncio
    async def test_create_use_case__non_existent_target_and_context(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test create use case with non-existent target and context"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent

        target_id = str(ObjectId())
        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["target_id"] = target_id
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert f'Target (id: "{target_id}") not found' in response.json()["detail"]

        context_id = str(ObjectId())
        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["context_id"] = context_id
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert f'Context (id: "{context_id}") not found' in response.json()["detail"]

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
        assert data["default_preview_table_id"] == str(new_ob_table_id_2)
        assert data["default_eda_table_id"] == str(new_ob_table_id_3)

        # test list observation tables endpoint
        response = test_api_client.get(
            f"{self.base_route}/{use_case_id}/observation_tables",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 3
        data = response.json()["data"]
        assert sorted(
            [str(new_ob_table_id_1), str(new_ob_table_id_2), str(new_ob_table_id_3)]
        ) == sorted([data[0]["_id"], data[1]["_id"], data[2]["_id"]])

        # test use case info endpoint
        response = test_api_client.get(
            f"{self.base_route}/{use_case_id}/info",
        )
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["name"] == self.payload["name"]
        assert data["description"] == self.payload["description"]
        assert data["primary_entities"] == [
            {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"}
        ]
        assert data["context_name"] == "transaction_context"
        assert data["target_name"] == "float_target"
        assert data["default_eda_table"] == "observation_table_from_target_input"
        assert data["default_preview_table"] == "observation_table_from_target_input"

    @pytest.mark.asyncio
    async def test_update_use_case_with_error(
        self,
        create_success_response,
        test_api_client_persistent,
        create_observation_table,
    ):
        """Test update use case with error"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        # create observation table with non target input
        non_target_ob_table_id = ObjectId()
        await create_observation_table(non_target_ob_table_id, target_input=False)
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"new_observation_table_id": str(non_target_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "observation table request_input is not TargetInput"

        different_target_ob_table_id = ObjectId()
        await create_observation_table(different_target_ob_table_id, same_target=False)
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"new_observation_table_id": str(different_target_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == "Inconsistent target_id between use case and observation table"
        )

        new_entity_id = ObjectId()
        entity_payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        entity_payload["_id"] = str(new_entity_id)
        entity_payload["name"] = "new_entity_name"
        entity_payload["serving_name"] = "new_entity_serving_name"
        response = test_api_client.post("/entity", json=entity_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        different_context_ob_table_id = ObjectId()
        context_payload = self.load_payload("tests/fixtures/request_payloads/context.json")
        context_payload["_id"] = str(different_context_ob_table_id)
        context_payload["primary_entity_ids"] = [str(new_entity_id)]
        context_payload["name"] = "new_context_name"
        response = test_api_client.post("/context", json=context_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        await create_observation_table(
            different_context_ob_table_id,
            same_context=False,
            new_context_id=different_context_ob_table_id,
        )
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"new_observation_table_id": str(different_context_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"]
            == "Inconsistent entities between use case and observation table"
        )

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
        assert response.json()["total"] == 1
        data = response.json()["data"]
        assert len(data) == 1
        assert data[0]["_id"] == str(feature_table_payload["_id"])
        assert data[0]["name"] == feature_table_payload["name"]
        assert data[0]["observation_table_id"] == str(new_ob_table_id)

    @pytest.mark.asyncio
    async def test_delete_associated_observation_table(
        self, test_api_client_persistent, create_success_response, create_observation_table
    ):
        """Test delete observation_table (fail) that is already associated with a use case"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        new_ob_table_id = ObjectId()
        await create_observation_table(new_ob_table_id)
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"new_observation_table_id": str(new_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.OK

        # delete the observation table that is associated with the use case
        response = test_api_client.delete(f"/observation_table/{str(new_ob_table_id)}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            f"Cannot delete Observation Table {str(new_ob_table_id)} because it is referenced"
            in response.json()["detail"]
        )

    @pytest.mark.asyncio
    async def test_delete_use_case(self, test_api_client_persistent, create_success_response):
        """Test delete observation_table (fail) that is already associated with a use case"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        response = test_api_client.get(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["_id"] == use_case_id

        # delete use case
        response = test_api_client.delete(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_list_deployments(
        self,
        create_success_response,
        test_api_client_persistent,
        user_id,
        default_catalog_id,
    ):
        """Test list deployments for use case"""
        test_api_client, persistent = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        await persistent.insert_one(
            collection_name="deployment",
            document={
                "name": "test_deployment",
                "catalog_id": ObjectId(default_catalog_id),
                "user_id": user_id,
                "feature_list_id": ObjectId(),
                "enabled": False,
                "context_id": ObjectId(),
                "use_case_id": ObjectId(use_case_id),
            },
            user_id=user_id,
        )

        response = test_api_client.get(f"{self.base_route}/{use_case_id}/deployments")
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["total"] == 1
        data = response.json()["data"]
        assert len(data) == 1
        assert data[0]["name"] == "test_deployment"
        assert data[0]["use_case_id"] == use_case_id

    @pytest.mark.asyncio
    async def test_create_use_case__target_and_context_with_different_entities(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test create use case with target and context having different entities"""

        _ = create_success_response
        test_api_client, _ = test_api_client_persistent

        entity_payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        entity_payload["_id"] = str(ObjectId())
        entity_payload["name"] = entity_payload["name"] + "_1"
        entity_payload["serving_name"] = entity_payload["serving_name"] + "_1"
        response = test_api_client.post("/entity", json=entity_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        context_payload = self.load_payload("tests/fixtures/request_payloads/context.json")
        context_payload["_id"] = str(ObjectId())
        context_payload["name"] = context_payload["name"] + "_1"
        context_payload["primary_entity_ids"] = [entity_payload["_id"]]
        response = test_api_client.post("/context", json=context_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        use_case_payload = self.load_payload("tests/fixtures/request_payloads/use_case.json")
        use_case_payload["_id"] = str(ObjectId())
        use_case_payload["name"] = use_case_payload["name"] + "_1"
        use_case_payload["context_id"] = context_payload["_id"]
        use_case_payload["target_id"] = create_success_response.json()["target_id"]
        response = test_api_client.post("/use_case", json=use_case_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert "Target and context must have the same entities" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_create_use_case__with_target_namespace_id(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test create use case with target and context having different entities"""

        _ = create_success_response
        test_api_client, _ = test_api_client_persistent

        target_id = create_success_response.json()["target_id"]
        response = test_api_client.get(f"/target/{target_id}")
        assert response.status_code == HTTPStatus.OK, response.json()
        target_namespace_id = response.json()["target_namespace_id"]

        use_case_payload = self.load_payload("tests/fixtures/request_payloads/use_case.json")
        use_case_payload["_id"] = str(ObjectId())
        use_case_payload["name"] = use_case_payload["name"] + "_1"
        use_case_payload["context_id"] = create_success_response.json()["context_id"]
        use_case_payload["target_namespace_id"] = target_namespace_id
        use_case_payload["target_id"] = None
        response = test_api_client.post("/use_case", json=use_case_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        response_dict = response.json()
        assert response_dict["name"] == use_case_payload["name"]
        assert response_dict["context_id"] == use_case_payload["context_id"]
        assert response_dict["target_namespace_id"] == target_namespace_id
        assert response_dict["target_id"] == target_id
