"""
Test for OnlineStore route
"""

from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestOnlineStoreApi(BaseApiTestSuite):
    """
    TestOnlineStoreApi
    """

    class_name = "OnlineStore"
    base_route = "/online_store"
    payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/mysql_online_store.json"
    )
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'OnlineStore (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `OnlineStore.get(name="mysql_online_store")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'OnlineStore (name: "mysql_online_store") already exists. '
            'Get the existing object by `OnlineStore.get(name="mysql_online_store")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {key: val for key, val in payload.items() if key != "name"},
            [
                {
                    "input": {key: val for key, val in payload.items() if key != "name"},
                    "loc": ["body", "name"],
                    "msg": "Field required",
                    "type": "missing",
                }
            ],
        )
    ]

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f"{self.payload['name']}_{i}"
            payload["details"] = {
                key: f"{value}_{i}" if key not in ["type", "credential"] else value
                for key, value in self.payload["details"].items()
            }
            yield payload

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert (
            response_dict.items()
            > {
                "name": "mysql_online_store",
                "updated_at": None,
                "details": {
                    "type": "mysql",
                    "host": "mysql_host",
                    "database": "mysql_database",
                    "port": 3306,
                    "credential": {
                        "type": "USERNAME_PASSWORD",
                        "username": "********",
                        "password": "********",
                    },
                },
            }.items()
        )
        assert "created_at" in response_dict

    def test_update_200(self, create_success_response, test_api_client_persistent):
        """
        Test online store update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]

        new_details = {
            "type": "mysql",
            "host": "mysql_host",
            "database": "mysql_database",
            "port": 3307,
            "credential": {
                "type": "USERNAME_PASSWORD",
                "username": "********",
                "password": "********",
            },
        }

        response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}", json={"details": new_details}
        )
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["name"] == "mysql_online_store"
        assert result["details"] == new_details

        response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}", json={"name": "new name"}
        )
        assert response.status_code == HTTPStatus.OK
        response = test_api_client.get(f"{self.base_route}/{catalog_id}")
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["name"] == "new name"
        assert result["details"] == new_details

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete online store"""
        test_api_client, _ = test_api_client_persistent
        online_store_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"{self.base_route}/{online_store_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check that online store is deleted
        response = test_api_client.get(f"{self.base_route}/{online_store_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_422(self, test_api_client_persistent, create_success_response):
        """Test delete online store (unsuccessful)"""
        test_api_client, _ = test_api_client_persistent
        online_store_id = create_success_response.json()["_id"]

        # create a catalog using the online store
        catalog_payload = self.load_payload("tests/fixtures/request_payloads/catalog.json")
        response = test_api_client.post("/catalog", json=catalog_payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        catalog_id = response.json()["_id"]
        response = test_api_client.patch(
            f"/catalog/{catalog_id}/online_store", json={"online_store_id": online_store_id}
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # attempt to delete the online store
        response = test_api_client.delete(f"{self.base_route}/{online_store_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "OnlineStore is referenced by Catalog: grocery"
