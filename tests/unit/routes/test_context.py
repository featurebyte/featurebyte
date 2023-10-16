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
                    "loc": ["body", "primary_entity_ids"],
                    "msg": "field required",
                    "type": "value_error.missing",
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
                    "loc": ["body", "__root__"],
                    "msg": "graph & node_name parameters must be specified together.",
                    "type": "value_error",
                }
            ],
        ),
        (
            {"node_name": "random_node"},
            [
                {
                    "loc": ["body", "__root__"],
                    "msg": "graph & node_name parameters must be specified together.",
                    "type": "value_error",
                }
            ],
        ),
        (
            {"graph": {"nodes": [], "edges": []}, "node_name": "input_1"},
            [
                {
                    "loc": ["body", "__root__"],
                    "msg": "node_name not exists in the graph.",
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
            ("item_table", "item_table"),
            ("event_table", "event_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

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
                    {"dtype": "INT", "name": "col_int"},
                    {"dtype": "FLOAT", "name": "col_float"},
                    {"dtype": "CHAR", "name": "col_char"},
                    {"dtype": "VARCHAR", "name": "col_text"},
                    {"dtype": "BINARY", "name": "col_binary"},
                    {"dtype": "BOOL", "name": "col_boolean"},
                    {"dtype": "TIMESTAMP", "name": "event_timestamp"},
                    {"dtype": "TIMESTAMP", "name": "created_at"},
                    {"dtype": "INT", "name": "cust_id"},
                ],
                "feature_store_details": {
                    "details": {
                        "account": "sf_account",
                        "database": "sf_database",
                        "sf_schema": "sf_schema",
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
            "observation_table_from_source_table", context_name=context.name
        )

        response = test_api_client.delete(f"{self.base_route}/{context.id}")
        assert response.json()["detail"] == (
            f"Context is associated with observation table: {observation_table.name}"
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
        target_payload = self.load_payload(f"tests/fixtures/request_payloads/target.json")
        response = test_api_client.post(f"/target", json=target_payload)
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
        assert response.json()["detail"] == "Context is used by use case: test_use_case"

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
            "featurebyte.routes.common.base.DerivePrimaryEntityHelper.derive_primary_entity_ids"
        ) as mock_derive:
            mock_derive.return_value = [ObjectId()]
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
            assert (
                f"Context entity ids must all be primary entity ids: {payload['primary_entity_ids']}"
                in response.json()["detail"]
            )

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
            assert (
                "Context entity ids must not include any parent entity ids"
                in response.json()["detail"]
            )
