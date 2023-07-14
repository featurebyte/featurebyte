"""
Tests for Entity route
"""
from http import HTTPStatus

import pytest
from bson.objectid import ObjectId

from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestEntityApi(BaseCatalogApiTestSuite):
    """
    TestEntityApi class
    """

    class_name = "Entity"
    base_route = "/entity"
    unknown_id = ObjectId()
    payload = BaseCatalogApiTestSuite.load_payload("tests/fixtures/request_payloads/entity.json")
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Entity (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `Entity.get(name="customer")`.',
        ),
        (
            {**payload, "_id": str(ObjectId())},
            'Entity (name: "customer") already exists. '
            'Get the existing object by `Entity.get(name="customer")`.',
        ),
        (
            {**payload, "_id": str(ObjectId()), "name": "whatever_name"},
            'Entity (serving_name: "cust_id") already exists. '
            'Get the existing object by `Entity.get(name="customer")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "serving_name": ["cust_id"]},
            [
                {
                    "loc": ["body", "serving_name"],
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
                "table_type": "event_table",
                "table_id": str(ObjectId()),
            },
            f'Entity (id: "{unknown_id}") not found. Please save the Entity object first.',
        )
    ]

    @pytest.fixture(name="create_multiple_entries")
    def create_multiple_entries_fixture(self, test_api_client_persistent):
        """
        Create multiple entries to the persistent
        """
        test_api_client, _ = test_api_client_persistent
        entity_id1, entity_id2, entity_id3 = str(ObjectId()), str(ObjectId()), str(ObjectId())
        res_region = test_api_client.post(
            self.base_route, json={"_id": entity_id1, "name": "region", "serving_name": "region"}
        )
        res_cust = test_api_client.post(
            self.base_route, json={"_id": entity_id2, "name": "customer", "serving_name": "cust_id"}
        )
        res_prod = test_api_client.post(
            self.base_route, json={"_id": entity_id3, "name": "product", "serving_name": "prod_id"}
        )
        assert res_region.status_code == HTTPStatus.CREATED
        assert res_cust.status_code == HTTPStatus.CREATED
        assert res_prod.status_code == HTTPStatus.CREATED
        assert res_region.json()["_id"] == entity_id1
        assert res_cust.json()["_id"] == entity_id2
        assert res_prod.json()["_id"] == entity_id3
        return [entity_id1, entity_id2, entity_id3]

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            payload["serving_name"] = f'{payload["serving_name"]}_{i}'
            yield payload

    @staticmethod
    def prepare_parent_payload(payload):
        """Create parent payload"""
        payload["table_type"] = "event_table"
        payload["table_id"] = str(ObjectId())
        return payload

    def test_update_200(self, create_success_response, test_api_client_persistent):
        """
        Test entity update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        entity_id = response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{entity_id}", json={"name": "Customer"}
        )
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["name"] == "Customer"

        # it is ok if the updated name is the same as the existing one
        response = test_api_client.patch(
            f"{self.base_route}/{entity_id}", json={"name": "Customer"}
        )
        assert response.status_code == HTTPStatus.OK

        # test get audit records
        response = test_api_client.get(f"{self.base_route}/audit/{entity_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        assert [record["previous_values"].get("name") for record in results["data"]] == [
            "customer",
            None,
        ]

        # test get name history
        response = test_api_client.get(f"{self.base_route}/history/name/{entity_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert [doc["name"] for doc in results] == ["Customer", "customer"]

    def test_update_404(self, test_api_client_persistent):
        """
        Test entity update (not found)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_entity_id = ObjectId()
        response = test_api_client.patch(
            f"{self.base_route}/{unknown_entity_id}", json={"name": "random_name"}
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json() == {
            "detail": (
                f'Entity (id: "{unknown_entity_id}") not found. Please save the Entity object first.'
            )
        }

    def test_update_409(self, create_multiple_entries, test_api_client_persistent):
        """ "
        Test entity update (conflict)
        """
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.patch(
            f"{self.base_route}/{create_multiple_entries[0]}", json={"name": "customer"}
        )
        assert response.status_code == HTTPStatus.CONFLICT
        assert response.json() == {
            "detail": (
                'Entity (name: "customer") already exists. '
                'Get the existing object by `Entity.get(name="customer")`.'
            )
        }

    def test_update_422(self, test_api_client_persistent):
        """
        Test entity update (unprocessable entity)
        """
        test_api_client, _ = test_api_client_persistent
        unknown_entity_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{unknown_entity_id}")
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
            "name": "customer",
            "updated_at": None,
            "serving_names": ["cust_id"],
            "catalog_name": "grocery",
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
