"""
Tests for Context route
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestContextApi(BaseApiTestSuite):
    """
    TestContextApi class
    """

    class_name = "Context"
    base_route = "/context"
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/context.json")
    unknown_id = ObjectId()
    create_conflict_payload_expected_detail_pairs = [
        (payload, f'Context (id: "{payload["_id"]}") already exists.')
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {"name": "some_context"},
            [
                {
                    "loc": ["body", "entity_ids"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ],
        ),
        (
            {**payload, "entity_ids": [str(unknown_id)]},
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
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("item_data", "item_data"),
            ("event_data", "event_data"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Payload generator to create multiple success response"""
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f"{payload['name']}_{i}"
            yield payload

    @pytest.fixture(name="input_event_data_node")
    def input_event_data_node_fixture(self):
        """Input event_data node of a graph"""
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
                "id": "6337f9651050ee7d5980660d",
                "id_column": "col_int",
                "table_details": {
                    "database_name": "sf_database",
                    "schema_name": "sf_schema",
                    "table_name": "sf_table",
                },
                "timestamp_column": "event_timestamp",
                "type": "event_data",
            },
        }

    @pytest.fixture(name="input_item_data_node")
    def input_item_data_node_fixture(self):
        """Input item_data node of a graph"""
        return {
            "name": "input_2",
            "type": "input",
            "output_type": "frame",
            "parameters": {
                "columns": [
                    {"dtype": "INT", "name": "event_id_col"},
                    {"dtype": "VARCHAR", "name": "item_id_col"},
                    {"dtype": "VARCHAR", "name": "item_type"},
                    {"dtype": "FLOAT", "name": "item_amount"},
                    {"dtype": "TIMESTAMP", "name": "created_at"},
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
                "id": "6337f9651050ee7d5980662d",
                "id_column": "item_id_col",
                "event_data_id": "6337f9651050ee7d5980660d",
                "event_id_column": "event_id_col",
                "table_details": {
                    "database_name": "sf_database",
                    "schema_name": "sf_schema",
                    "table_name": "items_table",
                },
                "type": "item_data",
            },
        }

    @pytest.fixture(name="join_node")
    def join_node_fixture(self):
        """Join node of a graph"""
        return {
            "name": "join_1",
            "type": "join",
            "output_type": "frame",
            "parameters": {
                "left_on": "col_int",
                "right_on": "event_id_col",
                "left_input_columns": ["event_timestamp", "cust_id", "col_int"],
                "left_output_columns": ["event_timestamp", "cust_id", "event_id_col"],
                "right_input_columns": ["event_id_col", "item_type", "item_amount"],
                "right_output_columns": ["event_id_col", "item_type", "item_amount"],
                "join_type": "inner",
                "scd_parameters": None,
            },
        }

    def test_update_200(
        self,
        create_success_response,
        test_api_client_persistent,
        input_event_data_node,
        input_item_data_node,
        join_node,
    ):
        """
        Test context update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        context_id = response_dict["_id"]
        graph = {
            "nodes": [input_event_data_node, input_item_data_node, join_node],
            "edges": [
                {"source": input_event_data_node["name"], "target": join_node["name"]},
                {"source": input_item_data_node["name"], "target": join_node["name"]},
            ],
        }
        payload = {"graph": graph, "node_name": join_node["name"]}
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
