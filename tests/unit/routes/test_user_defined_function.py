"""
Test for UserDefinedFunction route
"""
from http import HTTPStatus
from unittest import mock
from unittest.mock import Mock

import pytest
from bson import ObjectId

from featurebyte.enum import SourceType
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.session.base import BaseSession
from tests.unit.routes.base import BaseApiTestSuite


class TestUserDefinedFunctionApi(BaseApiTestSuite):
    """
    Test for UserDefinedFunction route
    """

    class_name = "UserDefinedFunction"
    base_route = "/user_defined_function"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/user_defined_function.json"
    )

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """Setup for creation route"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED

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

    @pytest.fixture(name="mock_get_session_to_throw_exception")
    def mock_snowflake_execute_to_throw_exception_fixture(
        self, session_manager, snowflake_execute_query
    ):
        """Mock get session to throw exception"""
        _, _ = session_manager, snowflake_execute_query
        with mock.patch(
            "featurebyte.service.session_manager.SessionManagerService.get_feature_store_session"
        ) as mocked_get_session:
            session = Mock(
                name="MockedSession",
                spec=BaseSession,
                source_type=SourceType.SNOWFLAKE,
            )
            session.check_user_defined_function.side_effect = Exception("Function not found")
            mocked_get_session.return_value = session
            yield mocked_get_session

    @staticmethod
    async def _update_feature_user_defined_function_ids(persistent, function_id, user_id):
        # check update function used by saved feature
        await persistent.update_many(
            collection_name="feature",
            query_filter={},
            update={"$set": {"user_defined_function_ids": [ObjectId(function_id)]}},
            user_id=user_id,
        )

    def test_create__function_not_found(
        self, test_api_client_persistent, mock_get_session_to_throw_exception
    ):
        """Test create route (function not found)"""
        _ = mock_get_session_to_throw_exception
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        response = self.post(test_api_client, self.payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "User defined function not exists: Function not found"

        # check the user defined function is not created
        response = test_api_client.get(f"{self.base_route}/{self.payload['_id']}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """Test update user defined function (success)"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]
        function_parameter = response_dict["function_parameters"][0]
        assert function_parameter["dtype"] == "FLOAT"

        # check update function parameter
        function_parameter["dtype"] = "INT"
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}", json={"function_parameters": [function_parameter]}
        )
        assert update_response.status_code == HTTPStatus.OK
        update_response_dict = update_response.json()
        assert update_response_dict["function_parameters"][0]["dtype"] == "INT"

    def test_update_404(self, test_api_client_persistent):
        """Test update user defined function (not found)"""
        test_api_client, _ = test_api_client_persistent

        random_id = ObjectId()
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{random_id}", json={"function_parameters": []}
        )
        assert update_response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_update_422(self, test_api_client_persistent, create_success_response):
        """Test update user defined function (unprocessable entity)"""
        test_api_client, persistent = test_api_client_persistent
        response_dict = create_success_response.json()
        doc_id = response_dict["_id"]
        function_parameter = response_dict["function_parameters"][0]
        assert function_parameter["name"] == "x"

        # check no changes found in function parameter
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}", json={"function_parameters": [function_parameter]}
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert update_response.json()["detail"] == "No changes found in function parameters"

        # check update non-exist function parameter
        function_parameter["name"] = "y"
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}", json={"function_parameters": [function_parameter]}
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert update_response.json()["detail"] == "Function parameter not exists: y"

        # check update function used by saved feature
        await self._update_feature_user_defined_function_ids(
            persistent=persistent, function_id=doc_id, user_id=response_dict["user_id"]
        )
        function_parameter.update({"name": "x", "dtype": "INT"})
        update_response = test_api_client.patch(
            url=f"{self.base_route}/{doc_id}", json={"function_parameters": [function_parameter]}
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            update_response.json()["detail"]
            == "User defined function used by saved feature(s): ['sum_30m']"
        )

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete user defined function (success)"""
        test_api_client, _ = test_api_client_persistent

        # test delete user defined function
        response = test_api_client.delete(
            url=f"{self.base_route}/{create_success_response.json()['_id']}"
        )
        assert response.status_code == HTTPStatus.OK

        # check the user defined function is deleted
        response = test_api_client.get(f"{self.base_route}/{create_success_response.json()['_id']}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_404(self, test_api_client_persistent):
        """Test delete user defined function (not found)"""
        test_api_client, _ = test_api_client_persistent

        random_id = ObjectId()
        response = test_api_client.delete(url=f"{self.base_route}/{random_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    @pytest.mark.asyncio
    async def test_delete_422(self, test_api_client_persistent, create_success_response):
        """Test delete user defined function (unprocessable entity)"""
        test_api_client, persistent = test_api_client_persistent
        response_dict = create_success_response.json()

        # check delete function used by saved feature
        await self._update_feature_user_defined_function_ids(
            persistent=persistent,
            function_id=response_dict["_id"],
            user_id=response_dict["user_id"],
        )
        response = test_api_client.delete(url=f"{self.base_route}/{response_dict['_id']}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            response.json()["detail"]
            == "User defined function used by saved feature(s): ['sum_30m']"
        )

    def test_list_200__filter_by_feature_store_id(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list route (200, filter by feature_store_id)"""
        test_api_client, _ = test_api_client_persistent

        # test filter by feature_store_id
        response = test_api_client.get(
            self.base_route, params={"feature_store_id": self.payload["feature_store_id"]}
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
                "feature_store_id": self.payload["feature_store_id"],
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
