"""
Tests for Credential route
"""
from http import HTTPStatus
from unittest.mock import patch

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
        (
            {**payload, "_id": str(ObjectId())},
            f'Credential (feature_store_id: "{payload["feature_store_id"]}") already exists. '
            f'Get the existing object by `Credential.get_by_id(id="{payload["_id"]}")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "name": ["test"]},
            [
                {
                    "loc": ["body", "name"],
                    "msg": "str type expected",
                    "type": "type_error.str",
                }
            ],
        )
    ]

    @pytest.fixture(autouse=True)
    def patch_validate_credentials(self):
        """
        Mock _validate_credential method
        """
        with patch("featurebyte.service.credential.CredentialService._validate_credential"):
            yield

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

        # delete credential stored for feature store
        response = api_client.get(f"{self.base_route}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        credential_id = results["data"][0]["_id"]
        response = api_client.delete(f"{self.base_route}/{credential_id}")
        assert response.status_code == HTTPStatus.OK

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client

        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            payload["feature_store_id"] = str(ObjectId())
            yield payload

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
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        result = response.json()

        # credentials should not exposed in response
        assert "database_credential" not in result
        assert "storage_credential" not in result

        # credential types will be exposed instead
        assert result["database_credential_type"] == "ACCESS_TOKEN"
        assert result["storage_credential_type"] == "S3"

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
                    "loc": ["body"],
                    "msg": "field required",
                    "type": "value_error.missing",
                }
            ]
        }

        response = test_api_client.patch(f"{self.base_route}/abc", json={"name": "anything"})
        assert response.json()["detail"] == [
            {
                "loc": ["path", self.id_field_name],
                "msg": "Id must be of type PydanticObjectId",
                "type": "type_error",
            }
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
                "database": "sf_database",
                "sf_schema": "sf_schema",
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
