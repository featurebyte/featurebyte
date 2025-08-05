"""
Tests for EventTable routes
"""

import copy
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson import ObjectId

from featurebyte.models.event_table import EventTableModel
from featurebyte.models.feature_store import TableStatus
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.model.table import EventTableData
from featurebyte.schema.event_table import EventTableCreate
from tests.unit.routes.base import BaseTableApiTestSuite


class TestEventTableApi(BaseTableApiTestSuite):
    """
    TestEventTableApi class
    """

    class_name = "EventTable"
    base_route = "/event_table"
    data_create_schema_class = EventTableCreate
    payload = BaseTableApiTestSuite.load_payload("tests/fixtures/request_payloads/event_table.json")
    document_name = "sf_event_table"
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

    @pytest_asyncio.fixture(name="event_timestamp_id_semantic_ids")
    async def event_timestamp_id_semantic_fixture(self, app_container):
        """Event timestamp & event ID semantic IDs fixture"""
        record_creation_timestamp = await app_container.semantic_service.get_or_create_document(
            "record_creation_timestamp"
        )
        event_timestamp = await app_container.semantic_service.get_or_create_document(
            "event_timestamp"
        )
        event_id = await app_container.semantic_service.get_or_create_document("event_id")
        return event_timestamp.id, event_id.id, record_creation_timestamp.id

    @pytest.fixture(name="data_model_dict")
    def data_model_dict_fixture(
        self,
        tabular_source,
        columns_info,
        user_id,
        event_timestamp_id_semantic_ids,
        feature_store_details,
        default_catalog_id,
    ):
        """Fixture for a Event Data dict"""
        (
            event_timestamp_semantic_id,
            event_id_semantic_id,
            record_creation_timestamp_id,
        ) = event_timestamp_id_semantic_ids
        cols_info = []
        for col_info in columns_info:
            col = col_info.copy()
            if col["name"] == "event_date":
                col["semantic_id"] = event_timestamp_semantic_id
            elif col["name"] == "event_id":
                col["semantic_id"] = event_id_semantic_id
            elif col["name"] == "created_at":
                col["semantic_id"] = record_creation_timestamp_id
            cols_info.append(col)

        event_table_dict = {
            "name": "订单表",
            "tabular_source": tabular_source,
            "columns_info": cols_info,
            "event_id_column": "event_id",
            "event_timestamp_column": "event_date",
            "record_creation_timestamp_column": "created_at",
            "default_feature_job_setting": {"blind_spot": "10m", "period": "30m", "offset": "5m"},
            "status": "PUBLISHED",
            "validation": {"status": "PASSED", "validation_message": None, "updated_at": None},
            "user_id": str(user_id),
            "_id": ObjectId(),
        }
        event_table_data = EventTableData(**event_table_dict)
        input_node = event_table_data.construct_input_node(
            feature_store_details=feature_store_details
        )
        graph = QueryGraph()
        inserted_node = graph.add_node(node=input_node, input_nodes=[])
        event_table_dict["graph"] = graph
        event_table_dict["node_name"] = inserted_node.name
        output = EventTableModel(**event_table_dict).json_dict()
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
                "blind_spot": "720s",
                "period": "1800s",
                "offset": "300s",
                "execution_buffer": "0s",
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
        data_model_dict["datetime_partition_column"] = None
        data_model_dict["datetime_partition_schema"] = None
        assert update_response_dict == data_model_dict

        # test get audit records
        response = test_api_client.get(f"/event_table/audit/{insert_id}")
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
            {"blind_spot": "600s", "period": "1800s", "offset": "300s", "execution_buffer": "0s"},
            None,
            None,
            None,
        ]

        # test get default_feature_job_setting_history
        response = test_api_client.get(
            f"/event_table/history/default_feature_job_setting/{insert_id}"
        )
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert [doc["setting"] for doc in results] == [
            {
                "blind_spot": "720s",
                "period": "1800s",
                "offset": "300s",
                "execution_buffer": "0s",
            },
            {
                "blind_spot": "600s",
                "period": "1800s",
                "offset": "300s",
                "execution_buffer": "0s",
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
        response = test_api_client.patch(f"/event_table/{insert_id}", json=data_update_dict)
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
        data_model_dict["datetime_partition_column"] = None
        data_model_dict["datetime_partition_schema"] = None
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

        for blind_spot in ["1m", "3m", "5m", "10m", "12m"]:
            response = test_api_client.patch(
                f"/event_table/{document_id}",
                json={
                    "default_feature_job_setting": {
                        "blind_spot": blind_spot,
                        "period": "30m",
                        "offset": "5m",
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
            f"/event_table/history/default_feature_job_setting/{document_id}"
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
            "event_timestamp_column": "event_timestamp",
            "record_creation_timestamp_column": "created_at",
            "table_details": {
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "table_name": "sf_table",
            },
            "default_feature_job_setting": {
                "blind_spot": "600s",
                "period": "1800s",
                "offset": "300s",
                "execution_buffer": "0s",
            },
            "status": "PUBLIC_DRAFT",
            "entities": [
                {
                    "name": "transaction",
                    "serving_names": ["transaction_id"],
                    "catalog_name": "grocery",
                },
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"},
            ],
            "semantics": ["event_id", "event_timestamp", "record_creation_timestamp"],
            "column_count": 9,
            "catalog_name": "grocery",
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["columns_info"] is None
        assert set(response_dict["semantics"]) == {
            "record_creation_timestamp",
            "event_timestamp",
            "event_id",
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
                "entity": "transaction",
                "semantic": "event_id",
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
                "semantic": "event_timestamp",
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
                "entity": "customer",
                "semantic": None,
                "critical_data_info": None,
                "description": None,
            },
        ]

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # untag id column's entity from the table first
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}/column_entity",
            json={"column_name": create_response_dict["event_id_column"], "entity_id": None},
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check deleted table
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_entity_and_semantic(self, test_api_client_persistent, create_success_response):
        """Test delete entity"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()

        # get the latest document
        response = test_api_client.get(f"/event_table/{create_response_dict['_id']}")
        columns_info = response.json()["columns_info"]
        cust_columns_info = next(col for col in columns_info if col["name"] == "cust_id")
        entity_id = cust_columns_info["entity_id"]
        assert entity_id is not None

        # attempt to delete entity
        response = test_api_client.delete(f"/entity/{entity_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Entity is referenced by Table: sf_event_table"

        # attempt to delete semantic
        col_int_columns_info = next(col for col in columns_info if col["name"] == "col_int")
        semantic_id = col_int_columns_info["semantic_id"]
        assert semantic_id is not None
        headers = test_api_client.headers.copy()
        response = test_api_client.delete(f"/semantic/{semantic_id}", headers=headers)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Semantic is referenced by Table: sf_event_table"

    @pytest_asyncio.fixture()
    async def table_with_semantic_id_not_found_in_persistent(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Table with semantic id not found in persistent"""
        test_api_client, persistent = test_api_client_persistent

        response_dict = create_success_response.json()
        table_id = ObjectId(response_dict["_id"])

        # update a column with non-existent semantic id
        col_idx = None
        for idx, (col_info1, col_info2) in enumerate(
            zip(self.payload["columns_info"], response_dict["columns_info"])
        ):
            if col_info1.get("entity_id") and not col_info2.get("semantic_id"):
                col_idx = idx
                break

        assert col_idx is not None
        column_info = response_dict["columns_info"][col_idx]
        assert column_info["semantic_id"] is None

        unknown_semantic_id = str(ObjectId())
        await persistent.update_one(
            collection_name="table",
            query_filter={"_id": table_id},
            update={"$set": {f"columns_info.{col_idx}.semantic_id": unknown_semantic_id}},
            user_id=user_id,
        )

        # check if the semantic id is updated
        response = test_api_client.get(f"{self.base_route}/{table_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["columns_info"][col_idx]["semantic_id"] == unknown_semantic_id
        return response_dict, col_idx, unknown_semantic_id

    def test_update_column_info_with_unknown_semantic_id(
        self, test_api_client_persistent, table_with_semantic_id_not_found_in_persistent
    ):
        """Test update entity"""
        test_api_client, _ = test_api_client_persistent
        table_dict, col_idx, semantic_id = table_with_semantic_id_not_found_in_persistent

        # take entity
        self.tag_table_entity(test_api_client, self.base_route.strip("/"), self.payload)

        # check updated table
        response = test_api_client.get(f"{self.base_route}/{table_dict['_id']}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["columns_info"][col_idx]["semantic_id"] == semantic_id
        for col1_info, col2_info in zip(
            response_dict["columns_info"], self.payload["columns_info"]
        ):
            assert col1_info["entity_id"] == col2_info["entity_id"]

        # check update critical data info
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}/column_critical_data_info",
            json={
                "column_name": "col_int",
                "critical_data_info": {
                    "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
                },
            },
        )
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["columns_info"][0]["critical_data_info"] == {
            "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
        }

        # check update column description
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}/column_description",
            json={"column_name": "col_int", "description": "Integer column"},
        )
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["columns_info"][0]["description"] == "Integer column"

    def test_deprecate_table_remove_entity_tagging(
        self, test_api_client_persistent, create_success_response
    ):
        """Test deprecate table and remove entity tagging"""
        test_api_client, _ = test_api_client_persistent
        table_id = create_success_response.json()["_id"]
        table_response = test_api_client.get(f"{self.base_route}/{table_id}")
        assert table_response.status_code == HTTPStatus.OK

        has_entity_tag = False
        for col in table_response.json()["columns_info"]:
            if col["entity_id"]:
                has_entity_tag = True
                break

        assert has_entity_tag, "Table should have entity tagging"
        response = test_api_client.patch(
            f"{self.base_route}/{table_id}",
            json={"status": "DEPRECATED"},
        )
        assert response.status_code == HTTPStatus.OK

        # check table status and if entity tagging is removed
        assert response.json()["status"] == "DEPRECATED"
        for col in response.json()["columns_info"]:
            assert col["entity_id"] is None

    def test_tag_same_entity_on_multiple_columns_422(
        self, test_api_client_persistent, create_success_response
    ):
        """Test tag same entity on multiple columns"""
        test_api_client, _ = test_api_client_persistent
        table_dict = create_success_response.json()
        columns = set(col_info["name"] for col_info in table_dict["columns_info"])
        table_id = table_dict["_id"]

        # tag 1st column with entity (expect success)
        entity_payload = self.load_payload("tests/fixtures/request_payloads/entity.json")
        col1 = "cust_id"
        assert col1 in columns
        response = test_api_client.patch(
            f"/event_table/{table_id}/column_entity",
            json={"column_name": col1, "entity_id": entity_payload["_id"]},
        )
        assert response.status_code == HTTPStatus.OK

        # tag 2nd column with same entity (expect failure)
        col2 = "col_int"
        assert col2 in columns
        response = test_api_client.patch(
            f"/event_table/{table_id}/column_entity",
            json={"column_name": col2, "entity_id": entity_payload["_id"]},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        expected_error = (
            f"Entity customer (ID: {entity_payload['_id']}) tagged to multiple columns "
            f"['col_int', 'cust_id'] in the table."
        )
        assert response.json()["detail"] == expected_error

    def test_cron_default_feature_job_setting(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = copy.deepcopy(self.payload)
        payload["default_feature_job_setting"] = {
            "crontab": "0 0 * * *",  # daily at midnight
            "timezone": "Etc/UTC",
            "reference_timezone": "Asia/Singapore",
        }
        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        assert response_dict["default_feature_job_setting"] == {
            "crontab": {
                "minute": 0,
                "hour": 0,
                "day_of_month": "*",
                "month_of_year": "*",
                "day_of_week": "*",
            },
            "timezone": "Etc/UTC",
            "reference_timezone": "Asia/Singapore",
            "blind_spot": None,
        }
