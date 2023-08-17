"""
Tests for SCDTable routes
"""
from http import HTTPStatus
from unittest import mock

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.scd_table import SCDTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import SCDTableData
from featurebyte.schema.scd_table import SCDTableCreate
from featurebyte.service.semantic import SemanticService
from tests.unit.routes.base import BaseTableApiTestSuite


class TestSCDTableApi(BaseTableApiTestSuite):
    """
    TestSCDTableApi class
    """

    class_name = "SCDTable"
    base_route = "/scd_table"
    data_create_schema_class = SCDTableCreate
    payload = BaseTableApiTestSuite.load_payload("tests/fixtures/request_payloads/scd_table.json")
    document_name = "sf_scd_table"
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
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'scd_table'}}\") "
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

    @pytest_asyncio.fixture(name="scd_table_semantic_ids")
    async def scd_table_semantic_ids_fixture(self, user_id, persistent):
        """SCD ID semantic IDs fixture"""
        user = mock.Mock()
        user.id = user_id
        semantic_service = SemanticService(user=user, persistent=persistent, catalog_id=None)
        natural_data = await semantic_service.get_or_create_document("natural_id")
        surrogate_data = await semantic_service.get_or_create_document("surrogate_id")
        return natural_data.id, surrogate_data.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self, tabular_source, columns_info, user_id, scd_table_semantic_ids, feature_store_details
    ):
        """Fixture for a SCD Data dict"""
        natural_id, surrogate_id = scd_table_semantic_ids
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "natural_id":
                col["semantic_id"] = natural_id
            elif col["name"] == "surrogate_id":
                col["semantic_id"] = surrogate_id
            cols_info.append(col)

        scd_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "record_creation_timestamp_column": "created_at",
            "status": "PUBLISHED",
            "user_id": str(user_id),
            "natural_key_column": "natural_id",
            "surrogate_key_column": "surrogate_id",
            "effective_timestamp_column": "effective_at",
            "end_timestamp_column": "end_at",
            "current_flag": "current_value",
        }
        scd_table_data = SCDTableData(**scd_table_dict)
        input_node = scd_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        scd_table_dict["graph"] = graph
        scd_table_dict["node_name"] = inserted_node.name
        output = SCDTableModel(**scd_table_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        SCD table update dict object
        """
        return {
            "record_creation_timestamp_column": "created_at",
            "natural_key_column": "natural_id",
            "surrogate_key_column": "surrogate_id",
            "effective_timestamp_column": "effective_at",
            "end_timestamp_column": "end_at",
            "current_flag": "current_value",
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
            "record_creation_timestamp_column": None,
            "current_flag_column": "is_active",
            "effective_timestamp_column": "effective_timestamp",
            "end_timestamp_column": "end_timestamp",
            "surrogate_key_column": "col_int",
            "natural_key_column": "col_text",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "scd_table",
            },
            "status": "PUBLIC_DRAFT",
            "entities": [],
            "semantics": [
                "scd_current_flag",
                "scd_effective_timestamp",
                "scd_end_timestamp",
                "scd_natural_key_id",
                "scd_surrogate_key_id",
            ],
            "column_count": 10,
            "catalog_name": "grocery",
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None
        assert set(response_dict["semantics"]) == {
            "scd_surrogate_key_id",
            "scd_end_timestamp",
            "scd_effective_timestamp",
            "scd_current_flag",
            "scd_natural_key_id",
        }

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
                "semantic": "scd_surrogate_key_id",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "col_float",
                "dtype": "FLOAT",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "is_active",
                "dtype": "BOOL",
                "entity": None,
                "semantic": "scd_current_flag",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "col_text",
                "dtype": "VARCHAR",
                "entity": None,
                "semantic": "scd_natural_key_id",
                "critical_data_info": None,
                "description": None,
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
                "name": "effective_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "entity": None,
                "semantic": "scd_effective_timestamp",
                "critical_data_info": None,
                "description": None,
            },
            {
                "name": "end_timestamp",
                "dtype": "TIMESTAMP_TZ",
                "entity": None,
                "semantic": "scd_end_timestamp",
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
                "name": "cust_id",
                "dtype": "INT",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
        ]
