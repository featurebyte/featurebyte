"""
Tests for Deployment route
"""
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from tests.unit.routes.base import BaseAsyncApiTestSuite, BaseCatalogApiTestSuite


class TestDeploymentApi(BaseAsyncApiTestSuite, BaseCatalogApiTestSuite):
    """
    TestDeploymentApi class
    """

    class_name = "Deployment"
    base_route = "/deployment"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/deployment.json"
    )
    async_create = True

    @pytest.fixture(autouse=True)
    def mock_online_enable_service_update_data_warehouse(self):
        """Mock _update_data_warehouse method in OnlineEnableService to make it a no-op"""
        with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
            yield

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """Setup for post route"""
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED, response.json()
            if api_object == "feature":
                self.make_feature_production_ready(api_client, response.json()["_id"], catalog_id)

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature_list_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_list_single.json"
        )
        for i in range(3):
            # make a new feature from feature_sum_30m & create a new feature_ids
            new_feature_id = str(ObjectId())
            feat_payload = feature_payload.copy()
            feat_payload["name"] = f"{feat_payload['name']}_{new_feature_id}"
            feat_payload["feature_namespace_id"] = str(ObjectId())
            response = api_client.post("/feature", json={**feat_payload, "_id": new_feature_id})
            assert response.status_code == HTTPStatus.CREATED
            assert response.json()["_id"] == new_feature_id
            self.make_feature_production_ready(api_client, new_feature_id, DEFAULT_CATALOG_ID)

            # save a new feature_list with the new feature_ids
            feat_list_payload = feature_list_payload.copy()
            feature_list_id = str(ObjectId())
            feat_list_payload["_id"] = feature_list_id
            feat_list_payload["name"] = f'{feat_list_payload["name"]}_{feature_list_id}'
            feat_list_payload["feature_ids"] = [new_feature_id]
            feat_list_payload["feature_list_namespace_id"] = str(ObjectId())
            response = api_client.post("/feature_list", json=feat_list_payload)
            assert response.status_code == HTTPStatus.CREATED

            # prepare the payload for deployment
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["feature_list_id"] = feature_list_id
            payload["name"] = f'{self.payload["name"]}_{i}'
            yield payload

    def test_list_200(self, test_api_client_persistent, create_multiple_success_responses):
        """Test list 200"""
        super().test_list_200(test_api_client_persistent, create_multiple_success_responses)

        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(self.base_route)
        response_dict = response.json()
        assert response_dict == {
            "data": [
                {
                    "_id": response_dict["data"][0]["_id"],
                    "name": 'Deployment (feature_list: "sf_feature_list")_2',
                    "feature_list_id": response_dict["data"][0]["feature_list_id"],
                    "enabled": False,
                    "catalog_id": str(DEFAULT_CATALOG_ID),
                    "user_id": response_dict["data"][0]["user_id"],
                    "created_at": response_dict["data"][0]["created_at"],
                    "updated_at": response_dict["data"][0]["updated_at"],
                },
                {
                    "_id": response_dict["data"][1]["_id"],
                    "name": 'Deployment (feature_list: "sf_feature_list")_1',
                    "feature_list_id": response_dict["data"][1]["feature_list_id"],
                    "enabled": False,
                    "catalog_id": str(DEFAULT_CATALOG_ID),
                    "user_id": response_dict["data"][1]["user_id"],
                    "created_at": response_dict["data"][1]["created_at"],
                    "updated_at": response_dict["data"][1]["updated_at"],
                },
                {
                    "_id": response_dict["data"][2]["_id"],
                    "name": 'Deployment (feature_list: "sf_feature_list")_0',
                    "feature_list_id": response_dict["data"][2]["feature_list_id"],
                    "enabled": False,
                    "catalog_id": str(DEFAULT_CATALOG_ID),
                    "user_id": response_dict["data"][2]["user_id"],
                    "created_at": response_dict["data"][2]["created_at"],
                    "updated_at": response_dict["data"][2]["updated_at"],
                },
            ],
            "page": 1,
            "page_size": 10,
            "total": 3,
        }

    def test_deployment_summary_200(self, test_api_client_persistent, create_success_response):
        """Test deployment summary"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get("/deployment/summary/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"num_feature_list": 0, "num_feature": 0}

        deployment_id = create_success_response.json()["_id"]
        test_api_client.patch(f"/deployment/{deployment_id}", json={"enabled": True})
        response = test_api_client.get("/deployment/summary/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"num_feature_list": 1, "num_feature": 1}

    def test_update_200__enable_and_disable_deployment(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update 200"""
        test_api_client, _ = test_api_client_persistent

        create_response_dict = create_success_response.json()
        deployment_id = create_response_dict["_id"]

        # enable deployment
        response = test_api_client.patch(
            f"{self.base_route}/{deployment_id}", json={"enabled": True}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["status"] == "SUCCESS", response_dict

        # get deployment and check if it is enabled
        response = test_api_client.get(f"{self.base_route}/{deployment_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["enabled"] is True

        # disable deployment
        response = test_api_client.patch(
            f"{self.base_route}/{deployment_id}", json={"enabled": False}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["status"] == "SUCCESS", response_dict

        # get deployment and check if it is disabled
        response = test_api_client.get(f"{self.base_route}/{deployment_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["enabled"] is False

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": 'Deployment (feature_list: "sf_feature_list")',
            "feature_list_name": "sf_feature_list",
            "feature_list_version": response_dict["feature_list_version"],
            "num_feature": 1,
            "enabled": False,
            "created_at": response_dict["created_at"],
            "updated_at": None,
        }
