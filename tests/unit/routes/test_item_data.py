"""
Tests for ItemData routes
"""
from http import HTTPStatus
from unittest import mock

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from featurebyte.models.item_data import ItemDataModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import ItemTableData
from featurebyte.schema.item_data import ItemDataCreate
from featurebyte.service.semantic import SemanticService
from tests.unit.routes.base import BaseDataApiTestSuite


class TestItemDataApi(BaseDataApiTestSuite):
    """
    TestsItemDataApi class
    """

    class_name = "ItemData"
    base_route = "/item_data"
    data_create_schema_class = ItemDataCreate
    payload = BaseDataApiTestSuite.load_payload("tests/fixtures/request_payloads/item_data.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'ItemData (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `ItemData.get(name="sf_item_data")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'ItemData (name: "sf_item_data") already exists. '
            'Get the existing object by `ItemData.get(name="sf_item_data")`.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "other_name"},
            f"ItemData (tabular_source: \"{{'feature_store_id': "
            f'ObjectId(\'{payload["tabular_source"]["feature_store_id"]}\'), \'table_details\': '
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'items_table'}}\") "
            'already exists. Get the existing object by `ItemData.get(name="sf_item_data")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "tabular_source": ("Some other source", "other table")},
            [
                {
                    "ctx": {"object_type": "TabularSource"},
                    "loc": ["body", "tabular_source"],
                    "msg": "value is not a valid TabularSource type",
                    "type": "type_error.featurebytetype",
                }
            ],
        ),
        (
            {**payload, "columns_info": 2 * payload["columns_info"]},
            [
                {
                    "loc": ["body", "columns_info"],
                    "msg": 'Column name "event_id_col" is duplicated.',
                    "type": "value_error",
                },
            ],
        ),
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(self, tabular_source, columns_info, user_id, feature_store_details):
        """Fixture for a Item Data dict"""
        item_data_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": columns_info,
            "event_id_column": "event_id",
            "item_id_column": "item_id",
            "event_data_id": str(ObjectId()),
            "status": "PUBLISHED",
            "user_id": str(user_id),
        }
        item_table_data = ItemTableData(**item_data_dict)
        input_node = item_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        item_data_dict["graph"] = graph
        item_data_dict["node_name"] = inserted_node.name
        output = ItemDataModel(**item_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """Item data update dict object"""
        return {"status": "PUBLISHED"}

    @pytest.mark.asyncio
    async def test_item_id_semantic(self, user_id, persistent, data_response):
        """Test item id semantic is set correctly"""
        user = mock.Mock()
        user.id = user_id
        semantic_service = SemanticService(
            user=user, persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
        )
        item_id_semantic = await semantic_service.get_or_create_document(name=SemanticType.ITEM_ID)

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
        # save event data first so that it can be referenced in get_item_data_info
        test_api_client, _ = test_api_client_persistent
        payload = BaseDataApiTestSuite.load_payload(
            "tests/fixtures/request_payloads/event_data.json"
        )
        response = test_api_client.post("/event_data", json=payload)
        assert response.status_code == HTTPStatus.CREATED

        # test item data info
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        expected_info_response = {
            "name": "sf_item_data",
            "record_creation_date_column": None,
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "items_table",
            },
            "status": "DRAFT",
            "entities": [],
            "semantics": ["item_id"],
            "column_count": 6,
            "event_data_name": "sf_event_data",
            "workspace_name": "default",
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
        assert verbose_response_dict["columns_info"] is not None
