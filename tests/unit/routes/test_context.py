"""
Tests for Context route
"""

from http import HTTPStatus
from unittest import mock

import pytest
from bson.objectid import ObjectId

from featurebyte.models import EntityModel
from featurebyte.models.entity import ParentEntity
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestContextApi(BaseCatalogApiTestSuite):
    """
    TestContextApi class
    """

    class_name = "Context"
    base_route = "/context"
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/context.json")
    unknown_id = ObjectId()
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'Context (id: "{payload["_id"]}") already exists.')
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {"name": "some_context"},
            [
                {
                    "input": {"name": "some_context"},
                    "loc": ["body", "primary_entity_ids"],
                    "msg": "Field required",
                    "type": "missing",
                }
            ],
        ),
        (
            {**payload, "primary_entity_ids": [str(unknown_id)]},
            f'Entity (id: "{unknown_id}") not found. Please save the Entity object first.',
        ),
    ]
    update_unprocessable_payload_expected_detail_pairs = [
        (
            {"graph": {"nodes": [], "edges": []}},
            [
                {
                    "input": {"graph": {"nodes": [], "edges": []}},
                    "ctx": {"error": {}},
                    "loc": ["body"],
                    "msg": "Value error, graph & node_name parameters must be specified together.",
                    "type": "value_error",
                }
            ],
        ),
        (
            {"node_name": "random_node"},
            [
                {
                    "input": {"node_name": "random_node"},
                    "ctx": {"error": {}},
                    "loc": ["body"],
                    "msg": "Value error, graph & node_name parameters must be specified together.",
                    "type": "value_error",
                }
            ],
        ),
        (
            {"graph": {"nodes": [], "edges": []}, "node_name": "input_1"},
            [
                {
                    "input": {"graph": {"nodes": [], "edges": []}, "node_name": "input_1"},
                    "ctx": {"error": {}},
                    "loc": ["body"],
                    "msg": "Value error, node_name not exists in the graph.",
                    "type": "value_error",
                }
            ],
        ),
    ]

    def pytest_generate_tests(self, metafunc):
        """Parametrize fixture at runtime"""
        super().pytest_generate_tests(metafunc)
        if "update_unprocessable_payload_expected_detail" in metafunc.fixturenames:
            metafunc.parametrize(
                "update_unprocessable_payload_expected_detail",
                self.update_unprocessable_payload_expected_detail_pairs,
            )

    def setup_creation_route(self, api_client):
        """Setup for post route"""
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("event_table", "event_table"),
            ("item_table", "item_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

            if api_object.endswith("_table"):
                # tag table entity for table objects
                self.tag_table_entity(api_client, api_object, payload)

    def multiple_success_payload_generator(self, api_client):
        """Payload generator to create multiple success response"""
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f"{payload['name']}_{i}"
            yield payload

    @pytest.fixture(name="input_event_table_node")
    def input_event_table_node_fixture(self):
        """Input event_table node of a graph"""
        event_table_payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        return {
            "name": "input_1",
            "type": "input",
            "output_type": "frame",
            "parameters": {
                "columns": [
                    {
                        "dtype": "INT",
                        "name": "col_int",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "FLOAT",
                        "name": "col_float",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "CHAR",
                        "name": "col_char",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "VARCHAR",
                        "name": "col_text",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "BINARY",
                        "name": "col_binary",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "BOOL",
                        "name": "col_boolean",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "TIMESTAMP",
                        "name": "event_timestamp",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "TIMESTAMP",
                        "name": "created_at",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                    {
                        "dtype": "INT",
                        "name": "cust_id",
                        "dtype_metadata": None,
                        "partition_metadata": None,
                        "nested_field_metadata": None,
                    },
                ],
                "feature_store_details": {
                    "details": {
                        "account": "sf_account",
                        "database_name": "sf_database",
                        "schema_name": "sf_schema",
                        "role_name": "TESTING",
                        "warehouse": "sf_warehouse",
                    },
                    "type": "snowflake",
                },
                "id": event_table_payload["_id"],
                "id_column": "col_int",
                "table_details": {
                    "database_name": "sf_database",
                    "schema_name": "sf_schema",
                    "table_name": "sf_table",
                },
                "timestamp_column": "event_timestamp",
                "type": "event_table",
                "event_timestamp_timezone_offset": None,
                "event_timestamp_timezone_offset_column": None,
                "event_timestamp_schema": None,
            },
        }

    def test_update_200(
        self,
        create_success_response,
        test_api_client_persistent,
        input_event_table_node,
    ):
        """
        Test context update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]
        graph = {"nodes": [input_event_table_node], "edges": []}
        payload = {"graph": graph, "node_name": input_event_table_node["name"]}
        response = test_api_client.patch(f"{self.base_route}/{context_id}", json=payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["graph"] == graph

    def test_update_404(self, test_api_client_persistent):
        """
        Test context update (not found)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_context_id = ObjectId()
        response = test_api_client.patch(
            f"{self.base_route}/{unknown_context_id}", json={"name": "random_name"}
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {
            "detail": (
                f'Context (id: "{unknown_context_id}") not found. Please save the Context object first.'
            )
        }

    def test_update_422(
        self,
        create_success_response,
        test_api_client_persistent,
        update_unprocessable_payload_expected_detail,
    ):
        """
        Test context update (unprocessable)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]
        unprocessible_payload, expected_message = update_unprocessable_payload_expected_detail
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}", json=unprocessible_payload
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == expected_message

    def test_delete_200(self, create_success_response, test_api_client_persistent):
        """Test context delete (success)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]
        response = test_api_client.delete(f"{self.base_route}/{context_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

    def test_delete_422__used_by_observation_table(
        self,
        test_api_client_persistent,
        snowflake_database_table,
        patched_observation_table_service,
        catalog,
        context,
    ):
        """Test context delete (unprocessable)"""
        _ = catalog, patched_observation_table_service
        test_api_client, _ = test_api_client_persistent
        test_api_client.headers["active-catalog-id"] = str(catalog.id)
        observation_table = snowflake_database_table.create_observation_table(
            "observation_table_from_source_table",
            context_name=context.name,
            columns_rename_mapping={"event_timestamp": "POINT_IN_TIME"},
        )

        response = test_api_client.delete(f"{self.base_route}/{context.id}")
        assert response.json()["detail"] == (
            f"Context is referenced by ObservationTable: {observation_table.name}"
        )

    def test_delete_422__used_by_use_case(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test context delete (unprocessable)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]

        # create target
        target_payload = self.load_payload("tests/fixtures/request_payloads/target.json")
        target_payload["target_type"] = "regression"
        response = test_api_client.post("/target", json=target_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # create use case
        response = test_api_client.post(
            "/use_case",
            json={
                "name": "test_use_case",
                "context_id": context_id,
                "target_id": target_payload["_id"],
            },
        )
        assert response.status_code == HTTPStatus.CREATED, response.json()

        # check delete context
        response = test_api_client.delete(f"{self.base_route}/{context_id}")
        assert response.json()["detail"] == "Context is referenced by UseCase: test_use_case"

    def test_create_context_with_treatment(
        self,
        create_success_response,
        test_api_client_persistent,
    ):
        """
        Test context with treatment
        """
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response.json()

        # create target
        treatment_payload = self.load_payload("tests/fixtures/request_payloads/treatment.json")
        response = test_api_client.post("/treatment", json=treatment_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        context_id = str(ObjectId())
        payload = self.payload.copy()
        payload["_id"] = context_id
        payload["name"] = f"{payload['name']}_with_treament"
        payload["treatment_id"] = treatment_payload["_id"]

        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()

        response = test_api_client.get(f"{self.base_route}/{context_id}/info")

        treatment_in_response = response.json()["treatment"]
        treatment_in_response.pop("created_at")

        assert treatment_in_response == {
            "name": "test_treatment",
            "updated_at": None,
            "description": None,
            "dtype": "INT",
            "treatment_type": "binary",
            "source": "randomized",
            "design": "simple-randomization",
            "time": "static",
            "time_structure": "instantaneous",
            "interference": "none",
            "treatment_labels": [0, 1],
            "control_label": 0,
            "propensity": {"granularity": "global", "knowledge": "design-known", "p_global": 0.5},
        }

        response = test_api_client.delete(f"{self.base_route}/{context_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        response = test_api_client.get(f"/treatment/{treatment_payload['_id']}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_create_context__not_all_primary_entity_ids(
        self,
        create_success_response,
        test_api_client_persistent,
    ):
        """
        Test context update (unprocessable)
        """
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response.json()

        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["name"] = f"{payload['name']}_1"

        with mock.patch(
            "featurebyte.routes.common.derive_primary_entity_helper.DerivePrimaryEntityHelper.derive_primary_entity_ids"
        ) as mock_derive:
            mock_derive.return_value = [ObjectId()]
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
            assert "Context entity ids must all be primary entity ids" in response.json()["detail"]

    def test_create_context__entity_parent_id_in_the_list(
        self,
        create_success_response,
        test_api_client_persistent,
    ):
        """
        Test context update (unprocessable)
        """
        test_api_client, _ = test_api_client_persistent
        _ = create_success_response.json()
        entity_payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        entity_payload["serving_names"] = [entity_payload["serving_name"]]

        payload = self.payload.copy()
        payload["_id"] = str(ObjectId())
        payload["name"] = f"{payload['name']}_1"

        with mock.patch("featurebyte.service.entity.EntityService.get_document") as mock_get_doc:
            mock_get_doc.return_value = EntityModel(
                **entity_payload,
                parents=[
                    ParentEntity(
                        id=ObjectId(entity_payload["_id"]),
                        table_type="event_table",
                        table_id=ObjectId(),
                    )
                ],
            )
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
            assert "Context entity ids must all be primary entity ids" in response.json()["detail"]

    def test_delete_entity(self, test_api_client_persistent, create_success_response):
        """Test delete entity fails when entity is referenced by context"""
        create_response_dict = create_success_response.json()
        entity_id = create_response_dict["primary_entity_ids"][0]

        # attempt to delete entity
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.delete(f"/entity/{entity_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "Entity is referenced by Context: transaction_context"

    @pytest.mark.asyncio
    async def test_remove_observation_table(
        self,
        create_success_response,
        test_api_client_persistent,
        create_observation_table,
    ):
        """
        Test remove observation table (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]

        ob_table_id_1 = ObjectId()
        await create_observation_table(ob_table_id_1)

        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={"observation_table_id_to_remove": str(ob_table_id_1)},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot remove Context {context_id} as it is not associated with ObservationTable {str(ob_table_id_1)}."
        )

        ob_table_id_2 = ObjectId()
        await create_observation_table(
            ob_table_id_2,
            context_id=context_id,
        )
        response = test_api_client.get(f"/observation_table/{ob_table_id_2}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["context_id"] == context_id

        # remove observation table from context
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={"observation_table_id_to_remove": str(ob_table_id_2)},
        )
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(f"/observation_table/{ob_table_id_2}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["context_id"] is None

    @pytest.mark.asyncio
    async def test_remove_observation_table__failed_obs_table_is_default_eda_or_preview(
        self,
        create_success_response,
        test_api_client_persistent,
        create_observation_table,
    ):
        """Test remove observation table (failed)"""

        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        context_id = create_response_dict["_id"]

        new_ob_table_id_1 = ObjectId()

        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id_1),
                "default_preview_table_id": str(new_ob_table_id_1),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            "observation_table_id_to_remove cannot be the same as default_preview_table_id"
            in response.json()["detail"][0]["msg"]
        )

        await create_observation_table(
            new_ob_table_id_1,
            context_id=context_id,
            entity_id=create_response_dict["primary_entity_ids"][0],
        )

        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "default_eda_table_id": str(new_ob_table_id_1),
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
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
            context_id=context_id,
            entity_id=create_response_dict["primary_entity_ids"][0],
        )

        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "default_preview_table_id": str(new_ob_table_id_2),
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        # delete use case from observation table
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "observation_table_id_to_remove": str(new_ob_table_id_2),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == f"Cannot remove observation_table {new_ob_table_id_2} as it is the default preview table"
        )

        # update context_id for new observation table with different primary entity ids
        new_ob_table_id_3 = ObjectId()
        await create_observation_table(new_ob_table_id_3, context_empty=True)
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "default_preview_table_id": str(new_ob_table_id_3),
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == "Cannot update Context as the primary_entity_ids are different from existing primary_entity_ids."
        )

    @pytest.mark.asyncio
    async def test_remove_default_table(
        self, test_api_client_persistent, create_success_response, create_observation_table
    ):
        """Test delete observation_table (fail) that is already associated with a context"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        context_id = create_response_dict["_id"]

        new_ob_table_id = ObjectId()
        await create_observation_table(
            new_ob_table_id,
            context_id=context_id,
        )

        # test create and remove default preview table
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "default_preview_table_id": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{context_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_preview_table_id"] == str(new_ob_table_id)

        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "remove_default_preview_table": True,
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.get(f"{self.base_route}/{context_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_preview_table_id"] is None

        # test create and remove default eda table
        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "default_eda_table_id": str(new_ob_table_id),
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{context_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_eda_table_id"] == str(new_ob_table_id)

        response = test_api_client.patch(
            f"{self.base_route}/{context_id}",
            json={
                "remove_default_eda_table": True,
            },
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{context_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["default_eda_table_id"] is None

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

    def test_update_user_provided_column_description__success(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update user-provided column description"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]

        # First add a user-provided column
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}",
            json={
                "user_provided_columns": [
                    {"name": "annual_income", "dtype": "FLOAT", "feature_type": "numeric"},
                ]
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # Update the description
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}/user_provided_column_description",
            json={"column_name": "annual_income", "description": "Customer's annual income"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert len(response.json()["user_provided_columns"]) == 1
        assert response.json()["user_provided_columns"][0]["name"] == "annual_income"
        assert (
            response.json()["user_provided_columns"][0]["description"] == "Customer's annual income"
        )

    def test_update_user_provided_column_description__column_not_found(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update user-provided column description when column not found"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]

        # First add a user-provided column
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}",
            json={
                "user_provided_columns": [
                    {"name": "annual_income", "dtype": "FLOAT", "feature_type": "numeric"},
                ]
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # Try to update a non-existent column
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}/user_provided_column_description",
            json={"column_name": "non_existent", "description": "Some description"},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert "User-provided column 'non_existent' not found" in response.json()["detail"]

    def test_update_user_provided_column_description__clear_description(
        self, test_api_client_persistent, create_success_response
    ):
        """Test clearing user-provided column description"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]

        # First add a user-provided column with description
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}",
            json={
                "user_provided_columns": [
                    {
                        "name": "annual_income",
                        "dtype": "FLOAT",
                        "feature_type": "numeric",
                        "description": "Customer's annual income",
                    },
                ]
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # Clear the description
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}/user_provided_column_description",
            json={"column_name": "annual_income", "description": None},
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["user_provided_columns"][0]["description"] is None
