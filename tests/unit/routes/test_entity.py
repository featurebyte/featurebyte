"""
Tests for Entity route
"""
from http import HTTPStatus

from bson.objectid import ObjectId

from tests.unit.routes.base import BaseApiTestSuite


class TestEntityApi(BaseApiTestSuite):
    """
    TestEntityApi class
    """

    class_name = "Entity"
    base_route = "/entity"
    payload_filename = "tests/fixtures/request_payloads/entity.json"
    payload = BaseApiTestSuite.load_payload(payload_filename)
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

    def test_update_200(self, create_success_response, test_api_client_persistent):
        """
        Test entity update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        entity_id = response_dict["_id"]
        response = test_api_client.patch(f"/entity/{entity_id}", json={"name": "Customer"})
        assert response.status_code == HTTPStatus.OK
        result = response.json()

        assert result["name"] == "Customer"
        assert len(result["name_history"]) == 1
        assert result["name_history"][0]["name"] == "customer"
        for key in result.keys():
            if key not in {"name", "name_history", "updated_at"}:
                assert result[key] == response_dict[key]

        # test special case when the name is the same, should not update name history
        response = test_api_client.patch(f"/entity/{entity_id}", json={"name": "Customer"})
        assert response.status_code == HTTPStatus.OK
        assert response.json() == result

        # test get audit records
        response = test_api_client.get(f"/entity/audit/{entity_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        print(results)
        assert results["total"] == 2
        assert [record["action_type"] for record in results["data"]] == ["UPDATE", "INSERT"]
        assert [record["previous_values"].get("name") for record in results["data"]] == [
            "customer",
            None,
        ]

        # test get default_feature_job_setting_history
        response = test_api_client.get(f"/entity/history/name/{entity_id}")
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
            f"/entity/{unknown_entity_id}", json={"name": "random_name"}
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
            f"/entity/{create_multiple_entries[0]}", json={"name": "customer"}
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
        response = test_api_client.patch(f"/entity/{unknown_entity_id}")
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
                f"/entity/{document_id}",
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
        response = test_api_client.get(f"/entity/history/name/{document_id}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert list(reversed(results)) == expected_history
