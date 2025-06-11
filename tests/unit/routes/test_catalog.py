"""
Tests for Catalog route
"""

from http import HTTPStatus

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.models.deployment import DeploymentModel
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
                    "input": ["test"],
                    "loc": ["body", "name"],
                    "msg": "Input should be a valid string",
                    "type": "string_type",
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
            f'Catalog (id: "{unknown_id}") not found. Please save the Catalog object first.',
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
            self.base_route,
            json={"_id": catalog_id1, "name": "grocery", "default_feature_store_ids": []},
        )
        res_creditcard = test_api_client.post(
            self.base_route,
            json={"_id": catalog_id2, "name": "creditcard", "default_feature_store_ids": []},
        )
        res_healthcare = test_api_client.post(
            self.base_route,
            json={"_id": catalog_id3, "name": "healthcare", "default_feature_store_ids": []},
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
        for _, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.CREATED
            output.append(response)
        return output

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    @staticmethod
    def prepare_parent_payload(payload):
        """Create parent payload"""
        payload["table_type"] = "event_table"
        payload["table_id"] = str(ObjectId())
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

    def test_update_online_store_200(
        self, create_success_response, test_api_client_persistent, mysql_online_store
    ):
        """
        Test catalog online store update (success) (to be deprecated)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store",
            json={"online_store_id": str(mysql_online_store.id)},
        )
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["online_store_id"] == str(mysql_online_store.id)

        response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store", json={"online_store_id": None}
        )
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["online_store_id"] is None

    def test_update_online_store_202(
        self, create_success_response, test_api_client_persistent, mysql_online_store
    ):
        """
        Test catalog online store update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]
        task_response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store_async",
            json={"online_store_id": str(mysql_online_store.id)},
        )
        assert task_response.status_code == HTTPStatus.ACCEPTED
        task_result = task_response.json()
        response = test_api_client.get(task_result["output_path"])
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["online_store_id"] == str(mysql_online_store.id)

        task_response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store_async", json={"online_store_id": None}
        )
        assert task_response.status_code == HTTPStatus.ACCEPTED
        task_result = task_response.json()
        response = test_api_client.get(task_result["output_path"])
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["online_store_id"] is None

    def test_update_online_store_no_op(
        self, create_success_response, test_api_client_persistent, mysql_online_store
    ):
        """
        Test catalog online store update (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]

        # Update online store
        task_response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store_async",
            json={"online_store_id": str(mysql_online_store.id)},
        )
        assert task_response.status_code == HTTPStatus.ACCEPTED
        task_result = task_response.json()
        response = test_api_client.get(task_result["output_path"])
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["online_store_id"] == str(mysql_online_store.id)

        # Update again with the same online store, should be no op.
        task_response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store_async",
            json={"online_store_id": str(mysql_online_store.id)},
        )
        assert task_response.status_code == HTTPStatus.OK
        assert task_response.json() is None

    def test_update_online_store_populate_offline_feature_tables(
        self, create_success_response, test_api_client_persistent, mysql_online_store
    ):
        """
        Test catalog online store update with populate_offline_feature_tables (success)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]

        # Update populate_offline_store_tables
        task_response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store_async",
            json={"populate_offline_feature_tables": True},
        )
        assert task_response.status_code == HTTPStatus.ACCEPTED
        task_result = task_response.json()
        response = test_api_client.get(task_result["output_path"])
        assert response.status_code == HTTPStatus.OK
        result = response.json()
        assert result["online_store_id"] is None
        assert result["populate_offline_feature_tables"] is True

        # Update again with the same setting, should be no op.
        task_response = test_api_client.patch(
            f"{self.base_route}/{catalog_id}/online_store_async",
            json={"populate_offline_feature_tables": True},
        )
        assert task_response.status_code == HTTPStatus.OK
        assert task_response.json() is None

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
            "detail": [{"input": None, "loc": ["body"], "msg": "Field required", "type": "missing"}]
        }

        response = test_api_client.patch(f"{self.base_route}/abc", json={"name": "anything"})
        assert response.json()["detail"] == [
            {
                "ctx": {"class": "ObjectId"},
                "input": "abc",
                "loc": ["path", "catalog_id", "is-instance[ObjectId]"],
                "msg": "Input should be an instance of ObjectId",
                "type": "is_instance_of",
            },
            {
                "ctx": {"error": {}},
                "input": "abc",
                "loc": ["path", "catalog_id", "chain[str,function-plain[validate()]]"],
                "msg": "Value error, Invalid ObjectId",
                "type": "value_error",
            },
        ]

    def test_soft_delete(self, create_success_response, test_api_client_persistent):
        """
        Test catalog delete (soft delete)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]

        # test soft delete
        response = test_api_client.delete(
            f"{self.base_route}/{catalog_id}", params={"soft_delete": True}
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # attempt to retrieve the catalog
        response = test_api_client.get(f"{self.base_route}/{catalog_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

        # check soft deleted catalog not in catalog list
        response = test_api_client.get(f"{self.base_route}")
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["data"] == []

        # create a new catalog with the same name should be ok
        response = test_api_client.post(
            self.base_route,
            json={
                "_id": str(ObjectId()),
                "name": response_dict["name"],
                "default_feature_store_ids": response_dict["default_feature_store_ids"],
            },
        )
        assert response.status_code == HTTPStatus.CREATED
        new_response_dict = response.json()

        # check retrieved catalog by name
        response = test_api_client.get(f"{self.base_route}", params={"name": response_dict["name"]})
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["data"][0]["_id"] == new_response_dict["_id"]

    @pytest.mark.asyncio
    async def test_soft_delete__with_active_deployment(
        self, create_success_response, test_api_client_persistent, app_container
    ):
        """
        Test catalog delete (soft delete)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]

        # create deployment
        app_container.deployment_service.catalog_id = ObjectId(catalog_id)
        deployment = await app_container.deployment_service.create_document(
            data=DeploymentModel(
                name="test_deployment",
                feature_list_id=ObjectId(),
                feature_list_namespace_id=ObjectId(),
                enabled=True,
                catalog_id=catalog_id,
            )
        )
        assert deployment.enabled is True
        assert deployment.catalog_id == ObjectId(catalog_id)

        # test soft delete
        response = test_api_client.delete(
            f"{self.base_route}/{catalog_id}", params={"soft_delete": True}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == (
            "Catalog cannot be deleted because it still has active deployment: test_deployment"
        )

    def test_hard_delete__not_implemented(
        self, create_success_response, test_api_client_persistent
    ):
        """
        Test catalog delete (hard delete)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = response_dict["_id"]

        # test soft delete
        response = test_api_client.delete(
            f"{self.base_route}/{catalog_id}", params={"soft_delete": False}
        )
        assert response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR, response.json()
        assert response.json()["detail"] == "Hard delete is not supported"

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
            expected_history.append({
                "created_at": update_response_dict["updated_at"],
                "name": name,
            })

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
