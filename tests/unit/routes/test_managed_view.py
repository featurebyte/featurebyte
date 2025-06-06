"""
Test for ManagedView route
"""

from http import HTTPStatus

import pytest
from bson import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestManagedViewApi(BaseCatalogApiTestSuite):
    """
    Test for ManagedView route
    """

    class_name = "ManagedView"
    base_route = "/managed_view"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/managed_view.json"
    )

    def multiple_success_payload_generator(self, api_client):
        """
        Multiple success payload generator
        """
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{payload["name"]}_{i}'
            yield payload

    @pytest.mark.parametrize(
        "sql,expected_error_detail",
        [
            (
                "INSERT INTO some_table VALUES (1, 2)",
                "SQL query must contain a single SELECT statement.",
            ),
            (
                "SELECT * FROM some_table; SELECT * FROM another_table",
                "SQL query must contain a single SELECT statement.",
            ),
            (
                "SELECT * FROM some_table; INSERT INTO some_table VALUES (1, 2)",
                "SQL query must contain a single SELECT statement.",
            ),
            (
                "SELECT FROM * AS some_table",
                (
                    "Invalid SQL statement: Expected table name but got <Token token_type: TokenType.STAR, text: *, "
                    "line: 1, col: 13, start: 12, end: 12, comments: []>. Line 1, Col: 13."
                ),
            ),
        ],
    )
    def test_create__invalid_sql(self, test_api_client_persistent, sql, expected_error_detail):
        """Test create route (invalid sql containing multiple statements)"""
        test_api_client, _ = test_api_client_persistent
        payload = self.payload.copy()
        payload["sql"] = sql
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"].startswith(expected_error_detail)

        # check the managed view is not created
        response = test_api_client.get(f"{self.base_route}/{self.payload['_id']}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """Test update managed view (success)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]

        # check update function parameter
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}", json={"name": "new name"}
        )
        assert update_response.status_code == HTTPStatus.OK
        update_response_dict = update_response.json()
        expected_response_dict = response_dict.copy()
        expected_response_dict["updated_at"] = update_response_dict["updated_at"]
        expected_response_dict["name"] = "new name"
        assert update_response_dict == expected_response_dict

    def test_update_404(self, test_api_client_persistent):
        """Test update managed view (not found)"""
        test_api_client, _ = test_api_client_persistent

        random_id = ObjectId()
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{random_id}", json={"name": "new name"}
        )
        assert update_response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete managed view (success)"""
        test_api_client, _ = test_api_client_persistent

        # test delete managed view
        response = test_api_client.delete(
            url=f"{self.base_route}/{create_success_response.json()['_id']}"
        )
        assert response.status_code == HTTPStatus.OK

        # check the managed view is deleted
        response = test_api_client.get(f"{self.base_route}/{create_success_response.json()['_id']}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_404(self, test_api_client_persistent):
        """Test delete managed view (not found)"""
        test_api_client, _ = test_api_client_persistent

        random_id = ObjectId()
        response = test_api_client.delete(url=f"{self.base_route}/{random_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_422(self, test_api_client_persistent, create_success_response):
        """Test delete managed view (unprocessable entity)"""
        test_api_client, persistent = test_api_client_persistent
        response_dict = create_success_response.json()

        # check delete view used by registered table
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        payload["tabular_source"] = response_dict["tabular_source"]
        response = test_api_client.post("/event_table", json=payload)
        assert response.status_code == HTTPStatus.CREATED

        response = test_api_client.delete(url=f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "ManagedView is referenced by Table: sf_event_table"

    def test_list_200__filter_by_feature_store_id(
        self, test_api_client_persistent, create_multiple_success_responses, default_catalog_id
    ):
        """Test list route (200, filter by feature_store_id)"""
        test_api_client, _ = test_api_client_persistent

        response = test_api_client.get(f"/catalog/{default_catalog_id}")
        assert response.status_code == HTTPStatus.OK
        feature_store_id = response.json()["default_feature_store_ids"][0]

        # test filter by feature_store_id
        response = test_api_client.get(
            self.base_route, params={"feature_store_id": feature_store_id}
        )
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["total"] == len(create_multiple_success_responses)
        expected_ids = [doc.json()["_id"] for doc in create_multiple_success_responses]
        assert set(doc["_id"] for doc in response_dict["data"]) == set(expected_ids)

        # test filter by name & feature_store_id
        response = test_api_client.get(
            self.base_route,
            params={
                "name": f'{self.payload["name"]}_0',
                "feature_store_id": feature_store_id,
            },
        )
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict["total"] == 1
        assert response_dict["data"][0]["name"] == f'{self.payload["name"]}_0'

        # test filter by random feature_store_id
        random_id = str(ObjectId())
        response = test_api_client.get(self.base_route, params={"feature_store_id": random_id})
        assert response.status_code == HTTPStatus.OK
        assert response.json()["total"] == 0

    def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test get info route (200)"""
        test_api_client, _ = test_api_client_persistent
        model_response_dict = create_success_response.json()

        # use view to register a table
        payload = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        payload["tabular_source"] = model_response_dict["tabular_source"]
        response = test_api_client.post("/event_table", json=payload)
        assert response.status_code == HTTPStatus.CREATED
        table_response_dict = response.json()
        table_id = table_response_dict["_id"]

        # test get info
        response = test_api_client.get(
            url=f"{self.base_route}/{create_success_response.json()['_id']}/info"
        )
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict == {
            "name": "My Managed View",
            "sql": "SELECT * FROM my_table",
            "table_details": model_response_dict["tabular_source"]["table_details"],
            "created_at": model_response_dict["created_at"],
            "description": "This is a managed view",
            "feature_store_name": "sf_featurestore",
            "updated_at": None,
            "used_by_tables": [{"id": table_id, "name": "sf_event_table"}],
        }

    def test_create_sql(
        self, test_api_client_persistent, create_success_response, snowflake_execute_query
    ):
        """Test create view SQL"""
        response_dict = create_success_response.json()
        view_name = response_dict["tabular_source"]["table_details"]["table_name"]

        assert snowflake_execute_query.call_args[1] == {
            "query": (
                f'CREATE VIEW "sf_database"."sf_schema"."{view_name}" AS\n'
                "SELECT * FROM (SELECT\n  *\nFROM my_table)"
            ),
            "timeout": 86400,
            "to_log_error": True,
        }

    def test_delete_sql(
        self, test_api_client_persistent, create_success_response, snowflake_execute_query
    ):
        """Test create view SQL"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        view_name = response_dict["tabular_source"]["table_details"]["table_name"]

        response = test_api_client.delete(url=f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.OK
        assert (
            snowflake_execute_query.call_args[0][0]
            == f'DROP TABLE "sf_database"."sf_schema"."{view_name}"'
        )
