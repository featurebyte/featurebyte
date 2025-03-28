"""
Tests for DimensionTable routes
"""

from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import DimensionTableData
from featurebyte.schema.dimension_table import DimensionTableCreate
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
    document_name = "sf_dimension_table"
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
                    "msg": 'Value error, Column name "col_int" is duplicated.',
                    "type": "value_error",
                }
            ],
        ),
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest_asyncio.fixture(name="dimension_id_semantic_id")
    async def dimension_id_semantic_id_fixture(self, app_container):
        """Dimension ID semantic IDs fixture"""
        dimension_id_semantic = await app_container.semantic_service.get_or_create_document(
            "dimension_id"
        )
        return dimension_id_semantic.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        dimension_id_semantic_id,
        feature_store_details,
    ):
        """Fixture for a Dimension Data dict"""
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "dimension_id":
                col["semantic_id"] = dimension_id_semantic_id
            cols_info.append(col)

        dimension_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "record_creation_timestamp_column": "created_at",
            "status": "PUBLISHED",
            "user_id": str(user_id),
            "dimension_id_column": "dimension_id",  # this value needs to match the column name used in test table
        }
        dimension_table_data = DimensionTableData(**dimension_table_dict)
        input_node = dimension_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        dimension_table_dict["graph"] = graph
        dimension_table_dict["node_name"] = inserted_node.name
        output = DimensionTableModel(**dimension_table_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Dimension table update dict object
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
            "name": self.document_name,
            "record_creation_timestamp_column": "created_at",
            "dimension_id_column": "col_int",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "dimension_table",
            },
            "status": "PUBLIC_DRAFT",
            "entities": [],
            "semantics": ["dimension_id", "record_creation_timestamp"],
            "column_count": 9,
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
                "name": "col_int",
                "dtype": "INT",
                "entity": None,
                "semantic": "dimension_id",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "col_float",
                "dtype": "FLOAT",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": "Float column",
            },
            {
                "name": "col_char",
                "dtype": "CHAR",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": "Char column",
            },
            {
                "name": "col_text",
                "dtype": "VARCHAR",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": "Text column",
            },
            {
                "name": "col_binary",
                "dtype": "BINARY",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "col_boolean",
                "dtype": "BOOL",
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
                "description": "Timestamp column",
            },
            {
                "name": "created_at",
                "dtype": "TIMESTAMP_TZ",
                "entity": None,
                "semantic": "record_creation_timestamp",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "cust_id",
                "dtype": "INT",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
        ]

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        table_id = create_response_dict["_id"]
        response = test_api_client.delete(f"{self.base_route}/{table_id}")
        assert response.status_code == HTTPStatus.OK, response.text

    def test_delete_422(self, test_api_client_persistent, create_success_response):
        """Test delete (unsuccessful)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        table_id = create_response_dict["_id"]
        entity_payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        response = test_api_client.patch(
            f"{self.base_route}/{table_id}/column_entity",
            json={
                "column_name": "col_int",
                "entity_id": entity_payload["_id"],
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # attempt to delete the table should fail
        response = test_api_client.delete(f"{self.base_route}/{table_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "DimensionTable is referenced by Entity: customer"
