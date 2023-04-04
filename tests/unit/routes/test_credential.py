"""
Tests for Credential route
"""
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", params={"catalog_id": catalog_id}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED

        # delete credential stored for feature store
        response = api_client.get(f"{self.base_route}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        credential_id = results["data"][0]["_id"]
        response = api_client.delete(f"{self.base_route}/{credential_id}")
        assert response.status_code == HTTPStatus.NO_CONTENT

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
                    "credential_type": "ACCESS_TOKEN",
                    "access_token": "test2",
                },
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        result = response.json()
        assert result["database_credential"] == {
            "credential_type": "ACCESS_TOKEN",
            "access_token": "test2",
        }

        # test get audit records
        response = test_api_client.get(f"{self.base_route}/audit/{credential_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        assert [
            record["previous_values"].get("database_credential") for record in results["data"]
        ] == [
            {"credential_type": "USERNAME_PASSWORD", "username": "user", "password": "pass"},
            None,
        ]

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
                    "credential_type": "ACCESS_TOKEN",
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
