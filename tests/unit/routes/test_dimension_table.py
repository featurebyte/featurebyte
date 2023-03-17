"""
Tests for DimensionTable routes
"""
from http import HTTPStatus
from unittest import mock

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import DimensionTableData
from featurebyte.schema.dimension_table import DimensionTableCreate
from featurebyte.service.semantic import SemanticService
from tests.unit.routes.base import BaseTableApiTestSuite


class TestDimensionTableApi(BaseTableApiTestSuite):
    """
    TestDimensionTableApi class
    """

    class_name = "DimensionTable"
    base_route = "/dimension_table"
    data_create_schema_class = DimensionTableCreate
    payload = BaseTableApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/dimension_table.json"
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
        ),
        (
            {**payload, "columns_info": 2 * payload["columns_info"]},
            [
                {
                    "loc": ["body", "columns_info"],
                    "msg": 'Column name "col_int" is duplicated.',
                    "type": "value_error",
                },
            ],
        ),
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest_asyncio.fixture(name="dimension_data_semantic_ids")
    async def dimension_data_semantic_ids_fixture(self, user_id, persistent):
        """Dimension ID semantic IDs fixture"""
        user = mock.Mock()
        user.id = user_id
        semantic_service = SemanticService(
            user=user, persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
        dimension_data = await semantic_service.get_or_create_document("dimension_id")
        return dimension_data.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        dimension_data_semantic_ids,
        feature_store_details,
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
            "record_creation_timestamp_column": "created_at",
            "status": "PUBLISHED",
            "user_id": str(user_id),
            "dimension_id_column": "dimension_id",  # this value needs to match the column name used in test data
        }
        dimension_table_data = DimensionTableData(**dimension_data_dict)
        input_node = dimension_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        dimension_data_dict["graph"] = graph
        dimension_data_dict["node_name"] = inserted_node.name
        output = DimensionTableModel(**dimension_data_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Dimension data update dict object
        """
        return {
            "record_creation_timestamp_column": "created_at",
        }

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        expected_info_response = {
            "name": "sf_dimension_data",
            "record_creation_timestamp_column": "created_at",
            "dimension_id_column": "col_int",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "status": "DRAFT",
            "entities": [],
            "semantics": ["dimension_id"],
            "column_count": 9,
            "catalog_name": "default",
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
