"""
Tests for ItemData routes
"""
import pytest
from bson.objectid import ObjectId

from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.item_data import ItemDataCreate
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
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'sf_table'}}\") "
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

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(self, tabular_source, columns_info, user_id):
        """Fixture for a Item Data dict"""
        event_data_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": columns_info,
            "event_id_column": "event_id",
            "item_id_column": "item_id",
            "status": "PUBLISHED",
            "user_id": str(user_id),
        }
        output = ItemDataModel(**event_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Item data update dict object
        """
        return {"status": "PUBLISHED"}
