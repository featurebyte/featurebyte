"""
Tests for Use Case route
"""

from http import HTTPStatus

import pytest
from bson import ObjectId

from featurebyte.query_graph.node.schema import TableDetails
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
                    "ctx": {"class": "ObjectId"},
                    "input": "test_id",
                    "loc": ["body", "context_id", "is-instance[ObjectId]"],
                    "msg": "Input should be an instance of ObjectId",
                    "type": "is_instance_of",
                },
                {
                    "ctx": {"error": {}},
                    "input": "test_id",
                    "loc": ["body", "context_id", "chain[str,function-plain[validate()]]"],
                    "msg": "Value error, Invalid ObjectId",
                    "type": "value_error",
                },
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

    @pytest.mark.asyncio
    async def test_list_observation_tables(
        self,
        test_api_client_persistent,
        create_observation_table,
        create_success_response,
    ):
        """Test automated assign observation table when creating use case"""
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response

        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["name"] = payload["name"] + "_1"
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        created_use_case = response.json()
        assert created_use_case["context_id"] == self.payload["context_id"]
        assert created_use_case["target_id"] == self.payload["target_id"]
        assert created_use_case["description"] == self.payload["description"]

        target_ob_table_id = ObjectId()
        non_target_ob_table_id = ObjectId()
        different_context_ob_table_id = ObjectId()

        # create observation table with target input
        await create_observation_table(
            target_ob_table_id,
            use_case_id=created_use_case["_id"],
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )
        # create observation table with non target input
        await create_observation_table(non_target_ob_table_id, target_input=False)
        # create observation table with different context
        await create_observation_table(different_context_ob_table_id)

        response = test_api_client.get(
            f"{self.base_route}/{created_use_case['_id']}/observation_tables",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 1
        assert response.json()["data"][0]["_id"] == str(target_ob_table_id)

    def test_create_use_case__non_existent_target_and_context(
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

        new_ob_table_id_2 = ObjectId()
        await create_observation_table(
            new_ob_table_id_2,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        new_ob_table_id_3 = ObjectId()
        await create_observation_table(
            new_ob_table_id_3,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

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
        assert response.json()["total"] == 2
        data = response.json()["data"]
        assert sorted([str(new_ob_table_id_2), str(new_ob_table_id_3)]) == sorted([
            data[0]["_id"],
            data[1]["_id"],
        ])

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
    async def test_delete_use_case__success(
        self,
        create_success_response,
        test_api_client_persistent,
        create_observation_table,
    ):
        """Test update use case"""

        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        new_ob_table_id = ObjectId()
        await create_observation_table(
            new_ob_table_id,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        # test list observation tables endpoint
        response = test_api_client.get(
            f"{self.base_route}/{use_case_id}/observation_tables",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 1
        assert response.json()["data"][0]["_id"] == str(new_ob_table_id)

        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(
            f"{self.base_route}/{use_case_id}/observation_tables",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 0

    @pytest.mark.asyncio
    async def test_delete_use_case__failed_use_case_id_not_linked(
        self,
        create_success_response,
        test_api_client_persistent,
        create_observation_table,
    ):
        """Test update use case"""

        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        new_ob_table_id = ObjectId()

        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id),
                "default_preview_table_id": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            "observation_table_id_to_remove cannot be the same as default_preview_table_id"
            in response.json()["detail"][0]["msg"]
        )

        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id),
                "default_eda_table_id": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            "observation_table_id_to_remove cannot be the same as default_eda_table_id"
            in response.json()["detail"][0]["msg"]
        )

        await create_observation_table(
            new_ob_table_id,
            use_case_id=ObjectId(),
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        # test list observation tables endpoint
        response = test_api_client.get(
            f"{self.base_route}/{use_case_id}/observation_tables",
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 0

        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"UseCase {use_case_id} is not associated with observation table {new_ob_table_id}"
        )

    @pytest.mark.asyncio
    async def test_delete_use_case__failed_obs_table_is_default_eda_or_preview(
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
        await create_observation_table(
            new_ob_table_id_1,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "default_eda_table_id": str(new_ob_table_id_1),
            },
        )
        assert response.status_code == HTTPStatus.OK

        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id_1),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot remove observation_table {new_ob_table_id_1} as it is the default EDA table"
        )

        new_ob_table_id_2 = ObjectId()
        await create_observation_table(
            new_ob_table_id_2,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "default_preview_table_id": str(new_ob_table_id_2),
            },
        )
        assert response.status_code == HTTPStatus.OK
        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id_2),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot remove observation_table {new_ob_table_id_2} as it is the default preview table"
        )

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

        different_context_ob_table_id = ObjectId()
        await create_observation_table(different_context_ob_table_id)
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"default_preview_table_id": str(different_context_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot add UseCase {use_case_id} due to mismatched contexts."
        )

        different_target_ob_table_id = ObjectId()
        await create_observation_table(
            different_target_ob_table_id, context_id=self.payload["context_id"]
        )
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={"default_preview_table_id": str(different_target_ob_table_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot add UseCase {use_case_id} due to mismatched targets."
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
        await create_observation_table(
            new_ob_table_id,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

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
        await create_observation_table(
            new_ob_table_id,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        # delete the observation table that is associated with the use case
        response = test_api_client.delete(f"/observation_table/{str(new_ob_table_id)}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"] == "ObservationTable is referenced by UseCase: test_use_case "
        )

    @pytest.mark.asyncio
    async def test_delete_use_case_422__use_case_used_in_observation_table(
        self, test_api_client_persistent, create_success_response, create_observation_table
    ):
        """Test delete use case that is already associated with a observation table"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        # create an observation table
        new_ob_table_id = ObjectId()
        await create_observation_table(
            new_ob_table_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )
        # add use case to the observation table
        response = test_api_client.patch(
            f"/observation_table/{new_ob_table_id}", json={"use_case_id_to_add": use_case_id}
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # delete use case
        response = test_api_client.delete(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "UseCase is referenced by ObservationTable: observation_table_from_target_input"
        )

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

    def test_create_use_case__target_and_context_with_different_entities(
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

    def test_create_use_case__with_target_namespace_id(
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

    @pytest.mark.asyncio
    async def test_remove_default_table(
        self, test_api_client_persistent, create_success_response, create_observation_table
    ):
        """Test delete observation_table (fail) that is already associated with a use case"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        use_case_id = create_response_dict["_id"]

        new_ob_table_id = ObjectId()
        await create_observation_table(
            new_ob_table_id,
            use_case_id=use_case_id,
            context_id=self.payload["context_id"],
            target_input=True,
            target_id=self.payload["target_id"],
        )

        # test create and remove default preview table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "default_preview_table_id": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_preview_table_id"] == str(new_ob_table_id)

        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "remove_default_preview_table": True,
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_preview_table_id"] is None

        # test create and remove default eda table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "default_eda_table_id": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_eda_table_id"] == str(new_ob_table_id)

        response = test_api_client.patch(
            f"{self.base_route}/{use_case_id}",
            json={
                "remove_default_eda_table": True,
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{use_case_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_eda_table_id"] is None

    def test_delete_target(self, test_api_client_persistent, create_success_response):
        """Test delete target (fail) that is already associated with a use case"""
        use_case_dict = create_success_response.json()
        target_id = use_case_dict["target_id"]
        assert target_id is not None

        # attempt to delete target
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.delete(f"/target/{target_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert (
            response.json()["detail"] == f"Target is referenced by UseCase: {use_case_dict['name']}"
        )

    @pytest.mark.asyncio
    async def test_update_name(self, test_api_client_persistent, create_success_response):
        """Test update name"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"name": "some other name"}
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["name"] == "some other name"

    @pytest.mark.asyncio
    async def test_update_eda_table_with_invalid_observation_table(
        self,
        api_client_persistent,
        create_success_response,
        create_observation_table,
    ):
        """Test update eda table with invalid observation table"""
        test_api_client, _ = api_client_persistent
        use_case = create_success_response.json()

        # create an observation table
        observation_table_id = ObjectId()
        await create_observation_table(
            ob_table_id=observation_table_id,
            use_case_id=use_case["_id"],
            context_id=use_case["context_id"],
            table_with_missing_data=TableDetails(
                database_name="test_db",
                schema_name="test_schema",
                table_name="test_table",
            ).model_dump(),
        )

        # check table created with missing data
        response = test_api_client.get(f"/observation_table/{observation_table_id}")
        assert response.status_code == HTTPStatus.OK
        observation_table = response.json()
        assert observation_table["table_with_missing_data"] is not None

        # attempt to use the observation table as eda table
        response = test_api_client.patch(
            f"{self.base_route}/{use_case['_id']}",
            json={"default_eda_table_id": str(observation_table_id)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": (
                f"Cannot set observation table {observation_table['_id']} as default EDA table as it is invalid. "
                "The table has missing data. Please check the table with missing data for more details. "
                "Please review the table for details and create a new table with all required data filled in."
            )
        }
