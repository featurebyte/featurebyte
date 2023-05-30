"""
Tests for Deployment route
"""
import json
from http import HTTPStatus
from unittest.mock import patch

import numpy as np
import pandas as pd
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
                    "name": "my_deployment_2",
                    "feature_list_id": response_dict["data"][0]["feature_list_id"],
                    "enabled": False,
                    "catalog_id": str(DEFAULT_CATALOG_ID),
                    "user_id": response_dict["data"][0]["user_id"],
                    "created_at": response_dict["data"][0]["created_at"],
                    "updated_at": response_dict["data"][0]["updated_at"],
                },
                {
                    "_id": response_dict["data"][1]["_id"],
                    "name": "my_deployment_1",
                    "feature_list_id": response_dict["data"][1]["feature_list_id"],
                    "enabled": False,
                    "catalog_id": str(DEFAULT_CATALOG_ID),
                    "user_id": response_dict["data"][1]["user_id"],
                    "created_at": response_dict["data"][1]["created_at"],
                    "updated_at": response_dict["data"][1]["updated_at"],
                },
                {
                    "_id": response_dict["data"][2]["_id"],
                    "name": "my_deployment_0",
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

    def test_all_deployment_200(
        self, test_api_client_persistent, create_success_response, catalog_id
    ):
        """Test listing all deployments"""
        deployment_id = create_success_response.json()["_id"]
        expected_version = f"V{pd.Timestamp.now().strftime('%y%m%d')}"
        expected_response = {
            "page": 1,
            "page_size": 10,
            "total": 1,
            "data": [
                {
                    "_id": deployment_id,
                    "name": "my_deployment",
                    "catalog_name": "default",
                    "feature_list_name": "sf_feature_list",
                    "feature_list_version": expected_version,
                    "num_feature": 1,
                }
            ],
        }

        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get("/deployment/all/?enabled=true")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"page": 1, "page_size": 10, "total": 0, "data": []}

        response = test_api_client.get("/deployment/all/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == expected_response

        # enable deployment
        self.update_deployment_enabled(test_api_client, deployment_id, DEFAULT_CATALOG_ID)

        # check all deployments again
        response = test_api_client.get("/deployment/all/?enabled=true")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == expected_response

        # check list deployments & all deployments again with different active catalog
        response = test_api_client.get(
            "/deployment/", headers={"active-catalog-id": str(catalog_id)}
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"page": 1, "page_size": 10, "total": 0, "data": []}

        response = test_api_client.get(
            "/deployment/all/?enabled=true", headers={"active-catalog-id": str(catalog_id)}
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json() == expected_response

    @staticmethod
    def check_feature_list_deployed(api_client, feature_list_id, expected_deployed):
        feature_list_response = api_client.get(f"/feature_list/{feature_list_id}")
        feature_list_dict = feature_list_response.json()
        assert feature_list_response.status_code == HTTPStatus.OK
        assert feature_list_dict["deployed"] == expected_deployed

    def test_update_200__enable_and_disable_single_deployment(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update 200"""
        test_api_client, _ = test_api_client_persistent

        create_response_dict = create_success_response.json()
        deployment_id = create_response_dict["_id"]
        feature_list_id = create_response_dict["feature_list_id"]

        # enable deployment & disable deployment
        self.update_deployment_enabled(test_api_client, deployment_id, DEFAULT_CATALOG_ID, True)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(test_api_client, deployment_id, DEFAULT_CATALOG_ID, False)
        self.check_feature_list_deployed(test_api_client, feature_list_id, False)

    def test_update_200__enable_and_disable_multiple_deployment_for_the_same_feature_list(
        self, test_api_client_persistent, create_success_response
    ):
        """Test multiple deployment for same feature list"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        catalog_id = DEFAULT_CATALOG_ID
        deployment_id = response_dict["_id"]
        feature_list_id = response_dict["feature_list_id"]

        # check create a new deployment with same feature list
        another_payload = self.payload.copy()
        another_payload.update({"name": "another_deployment", "_id": None})
        task_response = test_api_client.post(self.base_route, json=another_payload)
        task_response = self.wait_for_results(test_api_client, task_response)
        task_response_dict = task_response.json()
        assert task_response.status_code == HTTPStatus.OK
        assert task_response_dict["status"] == "SUCCESS"

        another_deployment_response = test_api_client.get(task_response_dict["output_path"])
        another_deployment_dict = another_deployment_response.json()
        assert another_deployment_dict["feature_list_id"] == feature_list_id
        another_deployment_id = another_deployment_dict["_id"]

        # enable deployments & check feature list deployed status
        self.update_deployment_enabled(test_api_client, deployment_id, catalog_id, True)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(test_api_client, another_deployment_id, catalog_id, True)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)

        # disable deployments & check feature list deployed status
        # note that the expected deployed status is True because at least one deployment is enabled
        self.update_deployment_enabled(test_api_client, deployment_id, catalog_id, False)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(test_api_client, another_deployment_id, catalog_id, False)
        self.check_feature_list_deployed(test_api_client, feature_list_id, False)

        # create enable & disable deployment again & check feature list deployed status
        self.update_deployment_enabled(test_api_client, another_deployment_id, catalog_id, True)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(test_api_client, another_deployment_id, catalog_id, False)
        self.check_feature_list_deployed(test_api_client, feature_list_id, False)

    def test_info_200(self, test_api_client_persistent, create_success_response):
        """Test info route"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.get(f"{self.base_route}/{doc_id}/info")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict == {
            "name": "my_deployment",
            "feature_list_name": "sf_feature_list",
            "feature_list_version": response_dict["feature_list_version"],
            "num_feature": 1,
            "enabled": False,
            "serving_endpoint": None,
            "created_at": response_dict["created_at"],
            "updated_at": None,
        }

    def test_get_online_features__200(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
    ):
        """Test feature list get_online_features"""
        test_api_client, _ = test_api_client_persistent

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([{"cust_id": 1, "feature_value": 123.0}])

        mock_session = mock_get_session.return_value
        mock_session.execute_query = mock_execute_query

        # Deploy feature list
        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], DEFAULT_CATALOG_ID)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            data=json.dumps(data),
        )
        assert response.status_code == HTTPStatus.OK, response.content

        # Check result
        assert response.json() == {"features": [{"cust_id": 1.0, "feature_value": 123.0}]}

    def test_get_online_features__not_deployed(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test feature list get_online_features"""
        test_api_client, _ = test_api_client_persistent
        deployment_doc = create_success_response.json()

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            data=json.dumps(data),
        )

        # Check error
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {"detail": "Feature List is not online enabled"}

    @pytest.mark.parametrize(
        "num_rows, expected_msg",
        [
            (1000, "ensure this value has at most 50 items"),
            (0, "ensure this value has at least 1 items"),
        ],
    )
    def test_get_online_features__invalid_number_of_rows(
        self,
        test_api_client_persistent,
        create_success_response,
        num_rows,
        expected_msg,
    ):
        """Test feature list get_online_features with invalid number of rows"""
        test_api_client, _ = test_api_client_persistent
        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], DEFAULT_CATALOG_ID)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}] * num_rows}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            data=json.dumps(data),
        )

        # Check error
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"][0]["msg"] == expected_msg

    @pytest.mark.parametrize(
        "missing_value",
        [np.nan, float("nan"), float("inf")],
    )
    def test_get_online_features__nan(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
        missing_value,
    ):
        """Test feature list get_online_features"""
        test_api_client, _ = test_api_client_persistent

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([{"cust_id": 1, "feature_value": missing_value}])

        mock_session = mock_get_session.return_value
        mock_session.execute_query = mock_execute_query

        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], DEFAULT_CATALOG_ID)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            data=json.dumps(data),
        )
        assert response.status_code == HTTPStatus.OK, response.content

        # Check result
        assert response.json() == {"features": [{"cust_id": 1.0, "feature_value": None}]}
