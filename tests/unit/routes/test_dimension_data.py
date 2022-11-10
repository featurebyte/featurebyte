"""
Tests for DimensionData routes
"""
from unittest import mock

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.dimension_data import DimensionDataModel
from featurebyte.schema.dimension_data import DimensionDataCreate
from featurebyte.service.semantic import SemanticService
from tests.unit.routes.base import BaseDataApiTestSuite


class TestDimensionDataApi(BaseDataApiTestSuite):
    """
    TestDimensionDataApi class
    """

    class_name = "DimensionData"
    base_route = "/dimension_data"
    data_create_schema_class = DimensionDataCreate
    payload = BaseDataApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/dimension_data.json"
    )
    document_name = "sf_dimension_data"
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
            f'ObjectId(\'{payload["tabular_source"]["feature_store_id"]}\'), \'table_details\': '
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'sf_table'}}\") "
            f'already exists. Get the existing object by `{class_name}.get(name="{document_name}")`.',
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

    @pytest_asyncio.fixture(name="dimension_data_semantic_ids")
    async def dimension_data_semantic_ids_fixture(self, user_id, persistent):
        """Dimension ID semantic IDs fixture"""
        user = mock.Mock()
        user.id = user_id
        semantic_service = SemanticService(user=user, persistent=persistent)
        dimension_data = await semantic_service.get_or_create_document("dimension_id")
        return dimension_data.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self, tabular_source, columns_info, user_id, dimension_data_semantic_ids
    ):
        """Fixture for a Dimension Data dict"""
        dimension_data_id = dimension_data_semantic_ids
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "dimension_id":
                col["semantic_id"] = dimension_data_id
            cols_info.append(col)

        dimension_data_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "record_creation_date_column": "created_at",
            "status": "PUBLISHED",
            "user_id": str(user_id),
            "dimension_data_id_column": "dimension_id",  # this value needs to match the column name used in test data
        }
        output = DimensionDataModel(**dimension_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Dimension data update dict object
        """
        return {
            "record_creation_date_column": "created_at",
        }
