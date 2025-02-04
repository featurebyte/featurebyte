"""
Tests for Credential route
"""

from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from featurebyte.models.credential import decrypt_value
from tests.unit.routes.base import BaseApiTestSuite


class TestCredentialApi(BaseApiTestSuite):
    """
    TestCredentialApi class
    """

    class_name = "Credential"
    base_route = "/credential"
    unknown_id = ObjectId()
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/credential.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Credential (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `Credential.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "name": ["test"]},
            [
                {
                    "input": ["test"],
                    "loc": ["body", "name"],
                    "msg": "Input should be a valid string",
                    "type": "string_type",
                }
            ],
        )
    ]

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client

        # Create featurestore
        feature_store = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        api_client.post("/feature_store", json=feature_store)

        # Create multiple credentials
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            payload["feature_store_id"] = feature_store["_id"]
            yield payload

    def create_multiple_success_responses_post_processing(self, api_client):
        """Post multiple success responses"""
        response = api_client.get(f"{self.base_route}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        for result in results["data"]:
            if result["name"] == "sf_featurestore":
                response = api_client.delete(f"{self.base_route}/{result['_id']}")
                assert response.status_code == HTTPStatus.OK

    def test_update_200(self, create_success_response, test_api_client_persistent):
        """
        Test credential update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        credential_id = response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{credential_id}",
            json={
                "database_credential": {
                    "type": "ACCESS_TOKEN",
                    "access_token": "test2",
                },
                "storage_credential": {
                    "type": "S3",
                    "s3_access_key_id": "test1",
                    "s3_secret_access_key": "test2",
                },
                "group_ids": [],
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        result = response.json()

        # credentials have values hidden
        assert result["database_credential"] == {"access_token": "********", "type": "ACCESS_TOKEN"}
        assert result["storage_credential"] == {
            "s3_access_key_id": "********",
            "s3_secret_access_key": "********",
            "type": "S3",
        }
        assert result["group_ids"] == []

        # test get audit records
        response = test_api_client.get(f"{self.base_route}/audit/{credential_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        previous_values = [
            record["previous_values"].get("database_credential") for record in results["data"]
        ]
        assert previous_values[0]["type"] == "USERNAME_PASSWORD"
        assert decrypt_value(previous_values[0]["username"]) == "user"
        assert decrypt_value(previous_values[0]["password"]) == "pass"
        assert previous_values[1] is None

    def test_update_group_ids_200(self, create_success_response, test_api_client_persistent):
        """
        Test credential update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        credential_id = response_dict["_id"]
        updated_group_ids = ["63eda344d0313fb925f7883b", "63eda344d0313fb925f7883a"]
        response = test_api_client.patch(
            f"{self.base_route}/{credential_id}",
            json={
                "database_credential": None,
                "storage_credential": None,
                "group_ids": updated_group_ids,
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        result = response.json()

        assert len(result["group_ids"]) == 2
        assert result["group_ids"] == updated_group_ids
        assert "database_credential" in result  # previous fields still exist
        assert "storage_credential" in result

        # test get audit records
        response = test_api_client.get(f"{self.base_route}/audit/{credential_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]

        previous_values = [record["previous_values"].get("group_ids") for record in results["data"]]
        current_values = [record["current_values"].get("group_ids") for record in results["data"]]
        assert previous_values[1] is None  # Created from null
        assert current_values[1] == []  # Default to empty list
        assert previous_values[0] == []
        assert (
            current_values[0] == updated_group_ids
        )  # Updated value must be equal to the updated group

    def test_update_404(self, test_api_client_persistent):
        """
        Test credential update (not found)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_credential_id = ObjectId()
        response = test_api_client.patch(
            f"{self.base_route}/{unknown_credential_id}",
            json={
                "database_credential": {
                    "type": "ACCESS_TOKEN",
                    "access_token": "test2",
                },
            },
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {
            "detail": (
                f'Credential (id: "{unknown_credential_id}") not found. Please save the Credential object first.'
            )
        }

    def test_update_422(self, test_api_client_persistent):
        """
        Test credential update (unprocessable credential)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_credential_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{unknown_credential_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": [
                {
                    "input": None,
                    "loc": ["body"],
                    "msg": "Field required",
                    "type": "missing",
                }
            ]
        }

        response = test_api_client.patch(f"{self.base_route}/abc", json={"name": "anything"})
        assert response.json()["detail"] == [
            {
                "ctx": {"class": "ObjectId"},
                "input": "abc",
                "loc": ["path", self.id_field_name, "is-instance[ObjectId]"],
                "msg": "Input should be an instance of ObjectId",
                "type": "is_instance_of",
            },
            {
                "ctx": {"error": {}},
                "input": "abc",
                "loc": ["path", self.id_field_name, "chain[str,function-plain[validate()]]"],
                "msg": "Value error, Invalid ObjectId",
                "type": "value_error",
            },
        ]

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
            "name": "grocery",
            "updated_at": None,
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        expected_feature_store_info = {
            "name": "sf_featurestore",
            "source": "snowflake",
            "database_details": {
                "account": "sf_account",
                "warehouse": "sf_warehouse",
                "database_name": "sf_database",
                "schema_name": "sf_schema",
                "role_name": "TESTING",
            },
        }
        assert response_dict["feature_store_info"].items() > expected_feature_store_info.items()

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        verbose_response_dict = verbose_response.json()
        assert verbose_response_dict.items() > expected_info_response.items(), verbose_response.text
        assert "created_at" in verbose_response_dict
