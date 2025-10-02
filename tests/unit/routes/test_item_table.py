"""
Tests for ItemTable routes
"""

from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.item_table import ItemTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import ItemTableData
from featurebyte.schema.item_table import ItemTableCreate
from tests.unit.routes.base import BaseTableApiTestSuite


class TestItemTableApi(BaseTableApiTestSuite):
    """
    TestsItemTableApi class
    """

    class_name = "ItemTable"
    base_route = "/item_table"
    data_create_schema_class = ItemTableCreate
    payload = BaseTableApiTestSuite.load_payload("tests/fixtures/request_payloads/item_table.json")
    document_name = "sf_item_table"
    random_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'{class_name} (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `{class_name}.get(name="{document_name}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            f'{class_name} (name: "{document_name}") already exists. '
            f'Get the existing object by `{class_name}.get(name="{document_name}")`.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "other_name"},
            f"{class_name} (tabular_source: \"{{'feature_store_id': "
            f"ObjectId('{payload['tabular_source']['feature_store_id']}'), 'table_details': "
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'items_table'}}\") "
            f'already exists. Get the existing object by `{class_name}.get(name="{document_name}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "tabular_source": ("Some other source", "other table")},
            [
                {
                    "input": ["Some other source", "other table"],
                    "loc": ["body", "tabular_source"],
                    "msg": "Input should be a valid dictionary or object to extract fields from",
                    "type": "model_attributes_type",
                }
            ],
        ),
        (
            {**payload, "columns_info": 2 * payload["columns_info"]},
            [
                {
                    "ctx": {"error": {}},
                    "input": 2 * payload["columns_info"],
                    "loc": ["body", "columns_info"],
                    "msg": 'Value error, Column name "event_id_col" is duplicated.',
                    "type": "value_error",
                }
            ],
        ),
        (
            {**payload, "event_table_id": random_id},
            f'EventTable (id: "{random_id}") not found. Please save the EventTable object first.',
        ),
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        feature_store_details,
        test_api_client_persistent,
    ):
        """Fixture for a Item Data dict"""
        event_table_payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post("/event_table", json=event_table_payload)
        assert response.status_code == HTTPStatus.CREATED

        item_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": columns_info,
            "event_id_column": "event_id",
            "item_id_column": "item_id",
            "event_table_id": event_table_payload["_id"],
            "status": "PUBLISHED",
            "user_id": str(user_id),
        }
        item_table_data = ItemTableData(**item_table_dict)
        input_node = item_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        item_table_dict["graph"] = graph
        item_table_dict["node_name"] = inserted_node.name
        output = ItemTableModel(**item_table_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """Item table update dict object"""
        return {"status": "PUBLISHED"}

    def setup_creation_route(self, api_client):
        super().setup_creation_route(api_client)
        api_object_filename_pairs = [
            ("event_table", "event_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    @pytest.mark.asyncio
    async def test_item_id_semantic(self, data_response, app_container):
        """Test item id semantic is set correctly"""
        item_id_semantic = await app_container.semantic_service.get_or_create_document(
            name=SemanticType.ITEM_ID.value
        )

        # check the that semantic ID is set correctly
        item_id_semantic_id = None
        response_dict = data_response.json()
        for col_info in response_dict["columns_info"]:
            if col_info["name"] == "item_id":
                item_id_semantic_id = col_info["semantic_id"]
        assert item_id_semantic_id == str(item_id_semantic.id)

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        # save event table first so that it can be referenced in get_item_table_info
        # test item table info
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        expected_info_response = {
            "name": self.document_name,
            "record_creation_timestamp_column": None,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "items_table",
            },
            "status": "PUBLIC_DRAFT",
            "entities": [
                {
                    "name": "transaction",
                    "serving_names": ["transaction_id"],
                    "catalog_name": "grocery",
                },
            ],
            "semantics": ["event_id", "item_id"],
            "column_count": 6,
            "event_table_name": "sf_event_table",
            "catalog_name": "grocery",
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        verbose_response_dict = verbose_response.json()
        assert verbose_response_dict.items() > expected_info_response.items(), verbose_response.text
        assert "created_at" in verbose_response_dict
        assert verbose_response_dict["columns_info"] == [
            {
                "name": "event_id_col",
                "dtype": "INT",
                "entity": "transaction",
                "semantic": "event_id",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "item_id_col",
                "dtype": "VARCHAR",
                "entity": None,
                "semantic": "item_id",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "item_type",
                "dtype": "VARCHAR",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "item_amount",
                "dtype": "FLOAT",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "created_at",
                "dtype": "TIMESTAMP_TZ",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "event_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
        ]

    def test_delete_event_table(self, test_api_client_persistent, create_success_response):
        """Test delete event table"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        event_table_id = create_response_dict["event_table_id"]
        response = test_api_client.delete(f"/event_table/{event_table_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "EventTable is referenced by ItemTable: sf_item_table"

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete item table"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        table_id = create_response_dict["_id"]
        response = test_api_client.delete(f"{self.base_route}/{table_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

    def test_event_id_column_missing_in_event_table_422(self, test_api_client_persistent):
        """
        Test event_id column missing in event table
        """
        test_api_client, _ = test_api_client_persistent
        event_table_payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        event_table_payload.pop("event_id_column")
        response = test_api_client.post("/event_table", json=event_table_payload)
        assert response.status_code == HTTPStatus.CREATED

        response = test_api_client.post(self.base_route, json=self.payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Event ID column not available in event table sf_event_table (ID: 6337f9651050ee7d5980660d)."
        )

    def test_event_id_column_dtype_mismatch_422(self, test_api_client_persistent):
        """
        Test event_id column dtype mismatch with that in event table
        """
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = self.payload.copy()
        payload["event_id_column"] = "item_type"
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Data type mismatch between event ID columns of event table sf_event_table "
            "(ID: 6337f9651050ee7d5980660d) (INT) and item table (VARCHAR)."
        )
