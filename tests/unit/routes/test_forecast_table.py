"""
Tests for ForecastTable routes
"""

from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import ForecastTableData
from featurebyte.schema.forecast_table import ForecastTableCreate
from tests.unit.routes.base import BaseTableApiTestSuite


class TestForecastTableApi(BaseTableApiTestSuite):
    """
    TestForecastTableApi class
    """

    class_name = "ForecastTable"
    base_route = "/forecast_table"
    data_create_schema_class = ForecastTableCreate
    payload = BaseTableApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/forecast_table.json"
    )
    document_name = "sf_forecast_table"
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
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'forecast_table'}}\") "
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
                    "msg": 'Value error, Column name "col_int" is duplicated.',
                    "type": "value_error",
                }
            ],
        ),
    ]
    update_unprocessable_payload_expected_detail_pairs = []

    @pytest_asyncio.fixture(name="forecast_semantic_ids")
    async def forecast_semantic_ids_fixture(self, app_container):
        """Fixture for forecast semantic ids"""
        record_creation_timestamp = await app_container.semantic_service.get_or_create_document(
            "record_creation_timestamp"
        )
        forecast_natural_key_id = await app_container.semantic_service.get_or_create_document(
            "forecast_natural_key_id"
        )
        forecast_effective_timestamp = await app_container.semantic_service.get_or_create_document(
            "forecast_effective_timestamp"
        )
        forecast_timestamp = await app_container.semantic_service.get_or_create_document(
            "forecast_timestamp"
        )
        return (
            record_creation_timestamp.id,
            forecast_natural_key_id.id,
            forecast_effective_timestamp.id,
            forecast_timestamp.id,
        )

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        forecast_semantic_ids,
        feature_store_details,
        default_catalog_id,
    ):
        """Fixture for a ForecastTable dict"""
        (
            record_creation_timestamp_id,
            forecast_natural_key_id,
            forecast_effective_timestamp_id,
            forecast_timestamp_id,
        ) = forecast_semantic_ids
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "event_id":
                col["semantic_id"] = forecast_natural_key_id
            elif col["name"] == "effective_at":
                col["semantic_id"] = forecast_effective_timestamp_id
            elif col["name"] == "end_at":
                col["semantic_id"] = forecast_timestamp_id
            elif col["name"] == "created_at":
                col["semantic_id"] = record_creation_timestamp_id
            cols_info.append(col)

        forecast_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "natural_key_column": "event_id",
            "effective_timestamp_column": "effective_at",
            "effective_timestamp_schema": None,
            "forecast_timestamp_column": "end_at",
            "forecast_timestamp_schema": None,
            "record_creation_timestamp_column": "created_at",
            "status": "PUBLISHED",
            "validation": {"status": "PASSED", "validation_message": None, "updated_at": None},
            "user_id": str(user_id),
            "_id": ObjectId(),
        }
        forecast_table_data = ForecastTableData(**forecast_table_dict)
        input_node = forecast_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        forecast_table_dict["graph"] = graph
        forecast_table_dict["node_name"] = inserted_node.name
        output = ForecastTableModel(**forecast_table_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        output["validation"].pop("updated_at")
        output["catalog_id"] = str(default_catalog_id)
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        ForecastTable update dict object
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
            "status": "PUBLIC_DRAFT",
            "catalog_name": "grocery",
            "record_creation_timestamp_column": "created_at",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "forecast_table",
            },
            "entities": [],
            "column_count": 9,
            "natural_key_column": "col_int",
            "effective_timestamp_column": "effective_timestamp",
            "effective_timestamp_schema": None,
            "forecast_timestamp_column": "forecast_timestamp",
            "forecast_timestamp_schema": None,
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None
        assert set(response_dict["semantics"]) == {
            "record_creation_timestamp",
            "forecast_natural_key_id",
            "forecast_effective_timestamp",
            "forecast_timestamp",
        }

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check deleted table
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
