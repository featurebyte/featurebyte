"""
Tests for Catalog route
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseApiTestSuite


class TestCatalogApi(BaseApiTestSuite):
    """
    TestCatalogApi class
    """

    class_name = "Catalog"
    base_route = "/catalog"
    unknown_id = ObjectId()
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/catalog.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Catalog (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `Catalog.get(name="grocery")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'Catalog (name: "grocery") already exists. '
            'Get the existing object by `Catalog.get(name="grocery")`.',
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
    create_parent_unprocessable_payload_expected_detail_pairs = [
        (
            {
                "id": str(unknown_id),
                "data_type": "event_table",
                "data_id": str(ObjectId()),
            },
            f'Catalog (id: "{unknown_id}") not found. Please save the Catalog object first.',
        )
    ]

    @pytest.fixture(name="create_multiple_entries")
    def create_multiple_entries_fixture(self, test_api_client_persistent):
        """
        Create multiple entries to the persistent
        """
        test_api_client, _ = test_api_client_persistent
        catalog_id1, catalog_id2, catalog_id3 = (
            str(ObjectId()),
            str(ObjectId()),
            str(ObjectId()),
        )
        res_grocery = test_api_client.post(
            self.base_route, json={"_id": catalog_id1, "name": "grocery"}
        )
        res_creditcard = test_api_client.post(
            self.base_route, json={"_id": catalog_id2, "name": "creditcard"}
        )
        res_healthcare = test_api_client.post(
            self.base_route, json={"_id": catalog_id3, "name": "healthcare"}
        )
        assert res_grocery.status_code == HTTPStatus.CREATED
        assert res_creditcard.status_code == HTTPStatus.CREATED
        assert res_healthcare.status_code == HTTPStatus.CREATED
        assert res_grocery.json()["_id"] == catalog_id1
        assert res_creditcard.json()["_id"] == catalog_id2
        assert res_healthcare.json()["_id"] == catalog_id3
        return [catalog_id1, catalog_id2, catalog_id3]

    @pytest_asyncio.fixture()
    async def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        output = []
        for index, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # skip first payload since default catalog is created automatically
            if index == 0:
                continue
            # payload name is set here as we need the exact name value for test_list_200 test
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.CREATED
            output.append(response)
        return output

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client

        # default catalog
        payload = self.payload.copy()
        payload["_id"] = str(DEFAULT_CATALOG_ID)
        payload["name"] = "default"
        yield payload

        for i in range(2):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    @staticmethod
    def prepare_parent_payload(payload):
        """Create parent payload"""
        payload["data_type"] = "event_table"
        payload["data_id"] = str(ObjectId())
        return payload

    def test_update_200(self, create_success_response, test_api_client_persistent):
        """
        Test catalog update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}", json={"name": "french grocery"}
        )
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["name"] == "french grocery"

        # it is ok if the updated name is the same as the existing one
        response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}", json={"name": "french grocery"}
        )
        assert response.status_code == HTTPStatus.OK

        # test get audit records
        response = test_api_client.get(f"{self.base_route}/audit/{catalog_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        assert [record["previous_values"].get("name") for record in results["data"]] == [
            "grocery",
            None,
        ]

        # test get name history
        response = test_api_client.get(f"{self.base_route}/history/name/{catalog_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert [doc["name"] for doc in results] == ["french grocery", "grocery"]

    def test_update_404(self, test_api_client_persistent):
        """
        Test catalog update (not found)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_catalog_id = ObjectId()
        response = test_api_client.patch(
            f"{self.base_route}/{unknown_catalog_id}", json={"name": "random_name"}
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {
            "detail": (
                f'Catalog (id: "{unknown_catalog_id}") not found. Please save the Catalog object first.'
            )
        }

    def test_update_409(self, create_multiple_entries, test_api_client_persistent):
        """ "
        Test catalog update (conflict)
        """
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.patch(
            f"{self.base_route}/{create_multiple_entries[0]}", json={"name": "creditcard"}
        )
        assert response.status_code == HTTPStatus.CONFLICT
        assert response.json() == {
            "detail": (
                'Catalog (name: "creditcard") already exists. '
                'Get the existing object by `Catalog.get(name="creditcard")`.'
            )
        }

    def test_update_422(self, test_api_client_persistent):
        """
        Test catalog update (unprocessable catalog)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_catalog_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{unknown_catalog_id}")
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

    def tests_get_name_history(self, test_api_client_persistent, create_success_response):
        """
        Test retrieve name history
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        document_id = response_dict["_id"]
        expected_history = [
            {
                "created_at": response_dict["created_at"],
                "name": response_dict["name"],
            }
        ]

        for name in ["a", "b", "c", "d", "e"]:
            response = test_api_client.patch(
                f"{self.base_route}/{document_id}",
                json={"name": name},
            )
            assert response.status_code == HTTPStatus.OK
            update_response_dict = response.json()
            expected_history.append(
                {
                    "created_at": update_response_dict["updated_at"],
                    "name": name,
                }
            )

        # test get default_feature_job_setting_history
        response = test_api_client.get(f"{self.base_route}/history/name/{document_id}")
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
            "name": "grocery",
            "updated_at": None,
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        verbose_response_dict = verbose_response.json()
        assert verbose_response_dict.items() > expected_info_response.items(), verbose_response.text
        assert "created_at" in verbose_response_dict
