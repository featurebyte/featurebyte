"""
Tests for ItemData routes
"""
from unittest import mock

import pytest
from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.item_data import ItemDataModel
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
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'sf_item_data_table'}}\") "
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
        )
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(self, tabular_source, columns_info, user_id):
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
        semantic_service = SemanticService(user=user, persistent=persistent)
        item_id_semantic = await semantic_service.get_or_create_document(name=SemanticType.ITEM_ID)

        # check the that semantic ID is set correctly
        item_id_semantic_id = None
        response_dict = data_response.json()
        for col_info in response_dict["columns_info"]:
            if col_info["name"] == "item_id":
                item_id_semantic_id = col_info["semantic_id"]
        assert item_id_semantic_id == str(item_id_semantic.id)
