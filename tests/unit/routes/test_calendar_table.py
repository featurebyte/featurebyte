"""
Tests for CalendarTable routes
"""

from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import CalendarTableData
from featurebyte.schema.calendar_table import CalendarTableCreate
from tests.unit.routes.base import BaseTableApiTestSuite


class TestCalendarTableApi(BaseTableApiTestSuite):
    """
    TestCalendarTableApi class
    """

    class_name = "CalendarTable"
    base_route = "/calendar_table"
    data_create_schema_class = CalendarTableCreate
    payload = BaseTableApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/calendar_table.json"
    )
    document_name = "sf_calendar_table"
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
            "{'database_name': 'sf_database', 'schema_name': 'sf_schema', 'table_name': 'calendar_table'}}\") "
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

    @pytest_asyncio.fixture(name="calendar_datetime_id_semantic_ids")
    async def calendar_datetime_id_semantic_ids_fixture(self, app_container):
        """Fixture for calendar datetime and series id semantic ids"""
        record_creation_timestamp = await app_container.semantic_service.get_or_create_document(
            "record_creation_timestamp"
        )
        calendar_date = await app_container.semantic_service.get_or_create_document("calendar_date")
        series_id = await app_container.semantic_service.get_or_create_document("series_id")
        return calendar_date.id, series_id.id, record_creation_timestamp.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        calendar_datetime_id_semantic_ids,
        feature_store_details,
        default_catalog_id,
    ):
        """Fixture for a CalendarTable dict"""
        (
            calendar_date_semantic_id,
            series_id_semantic_id,
            record_creation_timestamp_id,
        ) = calendar_datetime_id_semantic_ids
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "date":
                col["semantic_id"] = calendar_date_semantic_id
            elif col["name"] == "series_id":
                col["semantic_id"] = series_id_semantic_id
            elif col["name"] == "created_at":
                col["semantic_id"] = record_creation_timestamp_id
            cols_info.append(col)

        calendar_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "series_id_column": "series_id",
            "calendar_datetime_column": "date",
            "calendar_datetime_schema": {
                "format_string": "YYYY-MM-DD HH24:MI:SS",
                "timezone": "Etc/UTC",
                "is_utc_time": None,
            },
            "record_creation_timestamp_column": "created_at",
            "status": "PUBLISHED",
            "validation": {"status": "PASSED", "validation_message": None, "updated_at": None},
            "user_id": str(user_id),
            "_id": ObjectId(),
        }
        calendar_table_data = CalendarTableData(**calendar_table_dict)
        input_node = calendar_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        calendar_table_dict["graph"] = graph
        calendar_table_dict["node_name"] = inserted_node.name
        output = CalendarTableModel(**calendar_table_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        output["validation"].pop("updated_at")
        output["catalog_id"] = str(default_catalog_id)
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Calendar table update dict object
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
                "table_name": "calendar_table",
            },
            "entities": [],
            "column_count": 9,
            "series_id_column": "store_id",
            "calendar_datetime_column": "date",
            "calendar_datetime_schema": {
                "format_string": "YYYY-MM-DD HH24:MI:SS",
                "timezone": "Etc/UTC",
                "is_utc_time": None,
            },
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None
        assert set(response_dict["semantics"]) == {
            "record_creation_timestamp",
            "series_id",
            "calendar_date",
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

    def test_create_with_unsupported_reference_timestamp_schema(self, test_api_client_persistent):
        """
        Test create with unsupported calendar datetime schema (timezone in format string)
        """
        test_api_client, _ = test_api_client_persistent
        payload = self.payload.copy()
        payload["calendar_datetime_schema"] = {
            **payload["calendar_datetime_schema"],
            "format_string": "YYYY-MM-DD HH:MM:SS TZH:TZM",
        }
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_json = response.json()
        assert (
            response_json["detail"]
            == "Timezone information in calendar_datetime_column is not supported for CalendarTable."
        )
