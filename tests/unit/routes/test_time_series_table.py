"""
Tests for TimeSeriesTable routes
"""

from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.feature_store import TableStatus
from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import TimeSeriesTableData
from featurebyte.schema.time_series_table import TimeSeriesTableCreate
from tests.unit.routes.base import BaseTableApiTestSuite


class TestTimeSeriesTableApi(BaseTableApiTestSuite):
    """
    TestTimeSeriesTableApi class
    """

    class_name = "TimeSeriesTable"
    base_route = "/time_series_table"
    data_create_schema_class = TimeSeriesTableCreate
    payload = BaseTableApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/time_series_table.json"
    )
    document_name = "sf_time_series_table"
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

    @pytest_asyncio.fixture(name="reference_datetime_id_semantic_ids")
    async def reference_datetime_id_semantic_ids_fixture(self, app_container):
        """Reference datetime & series ID semantic IDs fixture"""
        record_creation_timestamp = await app_container.semantic_service.get_or_create_document(
            "record_creation_timestamp"
        )
        time_series_date_time = await app_container.semantic_service.get_or_create_document(
            "time_series_date_time"
        )
        series_id = await app_container.semantic_service.get_or_create_document("series_id")
        return time_series_date_time.id, series_id.id, record_creation_timestamp.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        reference_datetime_id_semantic_ids,
        feature_store_details,
        default_catalog_id,
    ):
        """Fixture for a Event Data dict"""
        (
            time_series_date_time_semantic_id,
            series_id_semantic_id,
            record_creation_timestamp_id,
        ) = reference_datetime_id_semantic_ids
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "date":
                col["semantic_id"] = time_series_date_time_semantic_id
            elif col["name"] == "series_id":
                col["semantic_id"] = series_id_semantic_id
            elif col["name"] == "created_at":
                col["semantic_id"] = record_creation_timestamp_id
            cols_info.append(col)

        time_series_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "series_id_column": "series_id",
            "reference_datetime_column": "date",
            "reference_datetime_schema": {
                "format_string": "YYYY-MM-DD HH24:MI:SS",
                "timezone": "Etc/UTC",
                "is_utc_time": None,
            },
            "time_interval": {"value": 1, "unit": "DAY"},
            "record_creation_timestamp_column": "created_at",
            "default_feature_job_setting": {
                "crontab": {
                    "minute": 0,
                    "hour": 0,
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                },
                "timezone": "Etc/UTC",
                "blind_spot": None,
            },
            "status": "PUBLISHED",
            "validation": {"status": "PASSED", "validation_message": None, "updated_at": None},
            "user_id": str(user_id),
            "_id": ObjectId(),
        }
        time_series_table_data = TimeSeriesTableData(**time_series_table_dict)
        input_node = time_series_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        time_series_table_dict["graph"] = graph
        time_series_table_dict["node_name"] = inserted_node.name
        output = TimeSeriesTableModel(**time_series_table_dict).json_dict()
        assert output.pop("created_at") is None
        assert output.pop("updated_at") is None
        output["validation"].pop("updated_at")
        output["catalog_id"] = str(default_catalog_id)
        return output

    @pytest.fixture(name="data_update_dict")
    def data_update_dict_fixture(self):
        """
        Event table update dict object
        """
        return {
            "default_feature_job_setting": {
                "crontab": {
                    "minute": 0,
                    "hour": 1,
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                },
                "timezone": "Etc/UTC",
                "reference_timezone": "Etc/UTC",
                "blind_spot": None,
            },
            "record_creation_timestamp_column": "created_at",
        }

    def test_update_success(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
        data_model_dict,
    ):
        """
        Update Event Data
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]

        response = test_api_client.patch(f"{self.base_route}/{insert_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.OK
        update_response_dict = response.json()
        assert update_response_dict["_id"] == insert_id
        update_response_dict.pop("created_at")
        update_response_dict.pop("updated_at")
        if "validation" in update_response_dict:
            update_response_dict["validation"].pop("updated_at")

        # default_feature_job_setting should be updated
        assert (
            update_response_dict.pop("default_feature_job_setting")
            == data_update_dict["default_feature_job_setting"]
        )

        # the other fields should be unchanged
        data_model_dict.pop("default_feature_job_setting")
        data_model_dict["status"] = TableStatus.PUBLIC_DRAFT
        assert update_response_dict == data_model_dict

        # test get audit records
        response = test_api_client.get(f"/time_series_table/audit/{insert_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 4
        assert [record["action_type"] for record in results["data"]] == [
            "UPDATE",
            "UPDATE",
            "UPDATE",
            "INSERT",
        ]
        assert [
            record["previous_values"].get("default_feature_job_setting")
            for record in results["data"]
        ] == [
            {
                "crontab": {
                    "minute": 0,
                    "hour": 0,
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                },
                "timezone": "Etc/UTC",
                "reference_timezone": None,
                "blind_spot": None,
            },
            None,
            None,
            None,
        ]

        # test get default_feature_job_setting_history
        response = test_api_client.get(
            f"/time_series_table/history/default_feature_job_setting/{insert_id}"
        )
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert [doc["setting"] for doc in results] == [
            {
                "crontab": {
                    "minute": 0,
                    "hour": 1,
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                },
                "timezone": "Etc/UTC",
                "reference_timezone": "Etc/UTC",
                "blind_spot": None,
            },
            {
                "crontab": {
                    "minute": 0,
                    "hour": 0,
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                },
                "timezone": "Etc/UTC",
                "reference_timezone": None,
                "blind_spot": None,
            },
        ]

    def test_update_excludes_unsupported_fields(
        self,
        test_api_client_persistent,
        data_response,
        data_update_dict,
        data_model_dict,
    ):
        """
        Update Event Data only updates job settings even if other fields are provided
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]
        assert insert_id

        # expect status to be draft
        assert response_dict["status"] == TableStatus.PUBLIC_DRAFT

        data_update_dict["name"] = "Some other name"
        data_update_dict["source"] = "Some other source"
        data_update_dict["status"] = TableStatus.PUBLISHED.value
        response = test_api_client.patch(f"/time_series_table/{insert_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.OK
        data = response.json()
        assert data["_id"] == insert_id
        data.pop("created_at")
        data.pop("updated_at")
        if "validation" in data:
            data["validation"].pop("updated_at")

        # default_feature_job_setting should be updated
        assert (
            data.pop("default_feature_job_setting")
            == data_update_dict["default_feature_job_setting"]
        )

        # the other fields should be unchanged
        data_model_dict.pop("default_feature_job_setting")
        assert data == data_model_dict

        # expect status to be updated to published
        assert data["status"] == TableStatus.PUBLISHED

    def test_get_default_feature_job_setting_history(
        self, test_api_client_persistent, data_response
    ):
        """
        Test retrieve default feature job settings history
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        document_id = response_dict["_id"]
        expected_history = [
            {
                "created_at": response_dict["created_at"],
                "setting": response_dict["default_feature_job_setting"],
            }
        ]

        for hour in ["1", "3", "4", "10", "12"]:
            response = test_api_client.patch(
                f"/time_series_table/{document_id}",
                json={
                    "default_feature_job_setting": {
                        "crontab": {
                            "minute": 0,
                            "hour": hour,
                            "day_of_week": "*",
                            "day_of_month": "*",
                            "month_of_year": "*",
                        },
                        "timezone": "Etc/UTC",
                    },
                    "status": "PUBLIC_DRAFT",
                },
            )
            assert response.status_code == HTTPStatus.OK
            update_response_dict = response.json()
            expected_history.append({
                "created_at": update_response_dict["updated_at"],
                "setting": update_response_dict["default_feature_job_setting"],
            })

        # test get default_feature_job_setting_history
        response = test_api_client.get(
            f"/time_series_table/history/default_feature_job_setting/{document_id}"
        )
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert list(reversed(results)) == expected_history

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
            "description": "test time series table",
            "status": "PUBLIC_DRAFT",
            "catalog_name": "grocery",
            "record_creation_timestamp_column": "created_at",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "time_series_table",
            },
            "entities": [],
            "column_count": 10,
            "series_id_column": "col_int",
            "reference_datetime_column": "date",
            "reference_datetime_schema": {
                "format_string": "YYYY-MM-DD HH24:MI:SS",
                "timezone": "Etc/UTC",
                "is_utc_time": None,
            },
            "time_interval": {"unit": "DAY", "value": 1},
            "default_feature_job_setting": None,
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None
        assert set(response_dict["semantics"]) == {
            "record_creation_timestamp",
            "series_id",
            "time_series_date_time",
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
                "semantic": "series_id",
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
                "name": "date",
                "dtype": "VARCHAR",
                "entity": None,
                "semantic": "time_series_date_time",
                "critical_data_info": None,
                "description": "Date column",
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
                "name": "store_id",
                "dtype": "INT",
                "entity": None,
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
            {
                "critical_data_info": None,
                "description": None,
                "dtype": "TIMESTAMP_TZ",
                "entity": None,
                "name": "another_timestamp_col",
                "semantic": None,
            },
        ]

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
        Test create with unsupported reference timestamp schema
        """
        test_api_client, _ = test_api_client_persistent
        payload = self.payload.copy()
        payload["reference_datetime_schema"]["format_string"] = "YYYY-MM-DD HH:MM:SS TZH:TZM"
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        response_json = response.json()
        assert (
            response_json["detail"]
            == "Timezone information in time series table reference datetime column is not supported."
        )
