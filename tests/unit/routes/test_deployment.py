"""
Tests for Deployment route
"""

import json
import textwrap
from datetime import datetime
from http import HTTPStatus
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from bson import ObjectId

from featurebyte.models.feature_materialize_run import FeatureMaterializeRun
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
    online_store_payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/mysql_online_store.json"
    )

    @pytest.fixture(autouse=True)
    def _mock_online_enable_service_update_data_warehouse(self, mock_deployment_flow):
        """Mock _update_data_warehouse method in OnlineEnableService to make it a no-op"""
        _ = mock_deployment_flow
        with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
            yield

    @pytest.fixture(autouse=True)
    def _always_patch_app_get_storage_fixture(self, storage):
        """
        Patch app.get_storage for all tests in this module
        """
        with patch(
            "featurebyte.routes.deployment.controller.DeploymentController._validate_deployment_is_online_enabled",
            return_value=None,
        ):
            yield

    @pytest.fixture(autouse=True)
    def _always_patch_deployment_online_fixture(self, storage):
        """
        Patch app.get_storage for all tests in this module
        """
        with patch("featurebyte.app.get_storage", return_value=storage):
            yield

    def setup_creation_route(self, api_client):
        """Setup for post route"""
        catalog_id = api_client.get("/catalog").json()["data"][0]["_id"]
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
            ("feature_list", "feature_list_single"),
            ("online_store", "mysql_online_store"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

            if api_object.endswith("_table"):
                # tag table entity for table objects
                self.tag_table_entity(api_client, api_object, payload)

            if api_object == "feature":
                self.make_feature_production_ready(api_client, response.json()["_id"], catalog_id)

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature_list_payload = self.load_payload(
            "tests/fixtures/request_payloads/feature_list_single.json"
        )
        default_catalog_id = api_client.get("/catalog").json()["data"][0]["_id"]
        for i in range(3):
            # make a new feature from feature_sum_30m & create a new feature_ids
            new_feature_id = str(ObjectId())
            feat_payload = feature_payload.copy()
            feat_payload["name"] = f"{feat_payload['name']}_{new_feature_id}"
            feat_payload["feature_namespace_id"] = str(ObjectId())
            response = api_client.post("/feature", json={**feat_payload, "_id": new_feature_id})
            assert response.status_code == HTTPStatus.CREATED
            assert response.json()["_id"] == new_feature_id
            self.make_feature_production_ready(api_client, new_feature_id, default_catalog_id)

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

    def update_catalog_online_store_id(self, api_client, catalog_id):
        """
        Add online store to catalog
        """
        response = api_client.patch(
            f"/catalog/{catalog_id}/online_store",
            json={"online_store_id": self.online_store_payload["_id"]},
        )
        assert response.status_code == HTTPStatus.OK, response.json()

    def test_list_200(self, test_api_client_persistent, create_multiple_success_responses):
        """Test list 200"""
        super().test_list_200(test_api_client_persistent, create_multiple_success_responses)

        test_api_client, _ = test_api_client_persistent
        default_catalog_id = test_api_client.headers["active-catalog-id"]
        response = test_api_client.get(self.base_route)
        response_dict = response.json()
        assert response_dict == {
            "data": [
                {
                    "_id": response_dict["data"][0]["_id"],
                    "name": "my_deployment_2",
                    "feature_list_id": response_dict["data"][0]["feature_list_id"],
                    "feature_list_namespace_id": response_dict["data"][0][
                        "feature_list_namespace_id"
                    ],
                    "enabled": False,
                    "catalog_id": default_catalog_id,
                    "user_id": response_dict["data"][0]["user_id"],
                    "created_at": response_dict["data"][0]["created_at"],
                    "updated_at": response_dict["data"][0]["updated_at"],
                    "block_modification_by": [],
                    "description": None,
                    "use_case_id": None,
                    "context_id": None,
                    "registry_info": None,
                    "serving_entity_ids": ["63f94ed6ea1f050131379214"],
                    "store_info": None,
                    "is_deleted": False,
                },
                {
                    "_id": response_dict["data"][1]["_id"],
                    "name": "my_deployment_1",
                    "feature_list_id": response_dict["data"][1]["feature_list_id"],
                    "feature_list_namespace_id": response_dict["data"][1][
                        "feature_list_namespace_id"
                    ],
                    "enabled": False,
                    "catalog_id": default_catalog_id,
                    "user_id": response_dict["data"][1]["user_id"],
                    "created_at": response_dict["data"][1]["created_at"],
                    "updated_at": response_dict["data"][1]["updated_at"],
                    "block_modification_by": [],
                    "description": None,
                    "use_case_id": None,
                    "context_id": None,
                    "registry_info": None,
                    "serving_entity_ids": ["63f94ed6ea1f050131379214"],
                    "store_info": None,
                    "is_deleted": False,
                },
                {
                    "_id": response_dict["data"][2]["_id"],
                    "name": "my_deployment_0",
                    "feature_list_id": response_dict["data"][2]["feature_list_id"],
                    "feature_list_namespace_id": response_dict["data"][2][
                        "feature_list_namespace_id"
                    ],
                    "enabled": False,
                    "catalog_id": default_catalog_id,
                    "user_id": response_dict["data"][2]["user_id"],
                    "created_at": response_dict["data"][2]["created_at"],
                    "updated_at": response_dict["data"][2]["updated_at"],
                    "block_modification_by": [],
                    "description": None,
                    "use_case_id": None,
                    "context_id": None,
                    "registry_info": None,
                    "serving_entity_ids": ["63f94ed6ea1f050131379214"],
                    "store_info": None,
                    "is_deleted": False,
                },
            ],
            "page": 1,
            "page_size": 10,
            "total": 3,
        }

    def test_list_200__filter_by_feature_list_id(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list by feature list id"""
        _ = create_multiple_success_responses

        test_api_client, _ = test_api_client_persistent
        default_catalog_id = test_api_client.headers["active-catalog-id"]

        response = test_api_client.get("/feature_list")
        assert response.status_code == HTTPStatus.OK, response.text
        fl_ids = [obj["_id"] for obj in response.json()["data"]]
        feature_list_namespace_ids = [
            obj["feature_list_namespace_id"] for obj in response.json()["data"]
        ]

        response = test_api_client.get(self.base_route, params={"feature_list_id": fl_ids[0]})
        response_dict = response.json()
        assert response_dict == {
            "data": [
                {
                    "_id": response_dict["data"][0]["_id"],
                    "block_modification_by": [],
                    "catalog_id": default_catalog_id,
                    "context_id": None,
                    "created_at": response_dict["data"][0]["created_at"],
                    "description": None,
                    "enabled": False,
                    "feature_list_id": fl_ids[0],
                    "feature_list_namespace_id": feature_list_namespace_ids[0],
                    "name": "my_deployment_2",
                    "updated_at": response_dict["data"][0]["updated_at"],
                    "use_case_id": None,
                    "user_id": response_dict["data"][0]["user_id"],
                    "registry_info": None,
                    "serving_entity_ids": ["63f94ed6ea1f050131379214"],
                    "store_info": None,
                    "is_deleted": False,
                }
            ],
            "page": 1,
            "page_size": 10,
            "total": 1,
        }

    def test_deployment_summary_200(
        self, test_api_client_persistent, create_success_response, default_catalog_id
    ):
        """Test deployment summary"""
        deployment_id = create_success_response.json()["_id"]
        test_api_client, _ = test_api_client_persistent
        # should work without active catalog id
        del test_api_client.headers["active-catalog-id"]
        response = test_api_client.get("/deployment/summary/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"num_feature_list": 0, "num_feature": 0}

        # enable deployment
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id)

        response = test_api_client.get("/deployment/summary/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"num_feature_list": 1, "num_feature": 1}

    def test_all_deployment_200(
        self, test_api_client_persistent, create_success_response, default_catalog_id, catalog_id
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
                    "catalog_name": "grocery",
                    "feature_list_name": "sf_feature_list",
                    "feature_list_version": expected_version,
                    "num_feature": 1,
                }
            ],
        }

        test_api_client, _ = test_api_client_persistent
        # should work without active catalog id
        del test_api_client.headers["active-catalog-id"]
        response = test_api_client.get("/deployment/all/?enabled=true")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {"page": 1, "page_size": 10, "total": 0, "data": []}

        response = test_api_client.get("/deployment/all/")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == expected_response

        # enable deployment
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id)

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

        response = test_api_client.get("/deployment/all/?enabled=true")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == expected_response

    @staticmethod
    def check_feature_list_deployed(api_client, feature_list_id, expected_deployed):
        """Check feature list deployed status"""
        feature_list_response = api_client.get(f"/feature_list/{feature_list_id}")
        feature_list_dict = feature_list_response.json()
        assert feature_list_response.status_code == HTTPStatus.OK
        assert feature_list_dict["deployed"] == expected_deployed

        for feature_id in feature_list_dict["feature_ids"]:
            feature_response = api_client.get(f"/feature/{feature_id}")
            assert feature_response.status_code == HTTPStatus.OK
            feature_dict = feature_response.json()
            assert feature_dict["online_enabled"] == expected_deployed

    def test_update_200__enable_and_disable_single_deployment(
        self, test_api_client_persistent, create_success_response, default_catalog_id
    ):
        """Test update 200"""
        test_api_client, _ = test_api_client_persistent

        create_response_dict = create_success_response.json()
        deployment_id = create_response_dict["_id"]
        feature_list_id = create_response_dict["feature_list_id"]

        # enable deployment & disable deployment
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id, True)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id, False)
        self.check_feature_list_deployed(test_api_client, feature_list_id, False)

    def test_update_200__enable_and_disable_multiple_deployment_for_the_same_feature_list(
        self, test_api_client_persistent, create_success_response, default_catalog_id
    ):
        """Test multiple deployment for same feature list"""
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
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
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id, True)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(
            test_api_client, another_deployment_id, default_catalog_id, True
        )
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)

        # disable deployments & check feature list deployed status
        # note that the expected deployed status is True because at least one deployment is enabled
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id, False)
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(
            test_api_client, another_deployment_id, default_catalog_id, False
        )
        self.check_feature_list_deployed(test_api_client, feature_list_id, False)

        # create enable & disable deployment again & check feature list deployed status
        self.update_deployment_enabled(
            test_api_client, another_deployment_id, default_catalog_id, True
        )
        self.check_feature_list_deployed(test_api_client, feature_list_id, True)
        self.update_deployment_enabled(
            test_api_client, another_deployment_id, default_catalog_id, False
        )
        self.check_feature_list_deployed(test_api_client, feature_list_id, False)

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete deployment"""
        test_api_client, _ = test_api_client_persistent
        deployment_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"{self.base_route}/{deployment_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check deleted deployment cannot be found
        response = test_api_client.get(f"{self.base_route}/{deployment_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_422(
        self, test_api_client_persistent, create_success_response, default_catalog_id
    ):
        """Test delete deployment (422)"""
        test_api_client, _ = test_api_client_persistent
        deployment_id = create_success_response.json()["_id"]
        self.update_deployment_enabled(test_api_client, deployment_id, default_catalog_id)
        response = test_api_client.delete(f"{self.base_route}/{deployment_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Only disabled deployment can be deleted."

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
            "description": None,
            "use_case_name": None,
        }

    def test_get_online_features__200(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
        default_catalog_id,
    ):
        """Test feature list get_online_features"""
        test_api_client, _ = test_api_client_persistent

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([{"cust_id": 1, "feature_value": 123.0, "__FB_TABLE_ROW_INDEX": 0}])

        mock_session = mock_get_session.return_value
        mock_session.execute_query_long_running = mock_execute_query

        # Deploy feature list
        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], default_catalog_id)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            content=json.dumps(data),
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
            content=json.dumps(data),
        )

        # Check error
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {"detail": "Deployment is not enabled"}

    @pytest.mark.parametrize(
        "num_rows, expected_msg",
        [
            (1000, "List should have at most 50 items after validation, not 1000"),
            (0, "List should have at least 1 item after validation, not 0"),
        ],
    )
    def test_get_online_features__invalid_number_of_rows(
        self,
        test_api_client_persistent,
        create_success_response,
        num_rows,
        expected_msg,
        default_catalog_id,
    ):
        """Test feature list get_online_features with invalid number of rows"""
        test_api_client, _ = test_api_client_persistent
        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], default_catalog_id)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}] * num_rows}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            content=json.dumps(data),
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
        default_catalog_id,
    ):
        """Test feature list get_online_features"""
        test_api_client, _ = test_api_client_persistent

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([
                {"cust_id": 1, "feature_value": missing_value, "__FB_TABLE_ROW_INDEX": 0}
            ])

        mock_session = mock_get_session.return_value
        mock_session.execute_query_long_running = mock_execute_query

        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], default_catalog_id)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            content=json.dumps(data),
        )
        assert response.status_code == HTTPStatus.OK, response.content

        # Check result
        assert response.json() == {"features": [{"cust_id": 1.0, "feature_value": None}]}

    def test_get_online_features__feast_store_not_available(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
        default_catalog_id,
    ):
        """Test feature list get_online_features"""
        test_api_client, _ = test_api_client_persistent

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([{"cust_id": 1, "feature_value": 123.0, "__FB_TABLE_ROW_INDEX": 0}])

        mock_session = mock_get_session.return_value
        mock_session.execute_query_long_running = mock_execute_query

        # Deploy feature list
        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], default_catalog_id)

        # Simulate online store being set, but feast registry is not available
        self.update_catalog_online_store_id(test_api_client, default_catalog_id)

        # Request online features
        deployment_id = deployment_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{deployment_id}/online_features",
            content=json.dumps(data),
        )
        assert response.status_code == HTTPStatus.OK, response.content

        # Check result
        assert response.json() == {"features": [{"cust_id": 1.0, "feature_value": 123.0}]}

    def test_request_sample_entity_serving_names(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
        default_catalog_id,
    ):
        """Test getting sample entity serving names for a deployment"""
        test_api_client, _ = test_api_client_persistent
        deployment_doc = create_success_response.json()
        self.update_deployment_enabled(test_api_client, deployment_doc["_id"], default_catalog_id)

        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = pd.DataFrame([
            {
                "cust_id": 1,
            },
            {
                "cust_id": 2,
            },
            {
                "cust_id": 3,
            },
        ])

        # Request sample entity serving names
        deployment_id = deployment_doc["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{deployment_id}/sample_entity_serving_names?count=10",
        )

        # Check result
        assert response.status_code == HTTPStatus.OK, response.content
        assert response.json() == {
            "entity_serving_names": [
                {"cust_id": "1"},
                {"cust_id": "2"},
                {"cust_id": "3"},
                {"cust_id": "1"},
                {"cust_id": "2"},
                {"cust_id": "3"},
                {"cust_id": "1"},
                {"cust_id": "2"},
                {"cust_id": "3"},
                {"cust_id": "1"},
            ],
        }

        # Check SQL
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                """
            WITH data AS (
              SELECT
                "col_int" AS "col_int",
                "col_float" AS "col_float",
                "col_char" AS "col_char",
                CAST("col_text" AS VARCHAR) AS "col_text",
                "col_binary" AS "col_binary",
                "col_boolean" AS "col_boolean",
                CAST("event_timestamp" AS VARCHAR) AS "event_timestamp",
                CAST("created_at" AS VARCHAR) AS "created_at",
                "cust_id" AS "cust_id"
              FROM "sf_database"."sf_schema"."sf_table"
            )
            SELECT
              DISTINCT "cust_id"
            FROM data
            WHERE
              "cust_id" IS NOT NULL
            LIMIT 10
            """
            ).strip()
        )

    @pytest.mark.asyncio
    async def test_get_job_history(
        self,
        test_api_client_persistent,
        create_success_response,
        default_catalog_id,
        app_container,
    ):
        """
        Test getting feature job history
        """
        test_api_client, _ = test_api_client_persistent
        deployment_doc = create_success_response.json()
        deployment_id = deployment_doc["_id"]

        app_container.feature_materialize_run_service.catalog_id = ObjectId(default_catalog_id)

        feature_table_id_1 = ObjectId()
        await app_container.feature_materialize_run_service.create_document(
            FeatureMaterializeRun(
                offline_store_feature_table_id=feature_table_id_1,
                offline_store_feature_table_name="customer",
                scheduled_job_ts=datetime(2024, 7, 15, 10, 0, 0),
                completion_ts=datetime(2024, 7, 15, 10, 10, 0),
                completion_status="success",
                duration_from_scheduled_seconds=10,
                deployment_ids=[ObjectId(deployment_id)],
            )
        )
        await app_container.feature_materialize_run_service.create_document(
            FeatureMaterializeRun(
                offline_store_feature_table_id=feature_table_id_1,
                offline_store_feature_table_name="customer",
                scheduled_job_ts=datetime(2024, 7, 15, 10, 10, 0),
                completion_ts=datetime(2024, 7, 15, 10, 25, 0),
                completion_status="success",
                duration_from_scheduled_seconds=15,
                deployment_ids=[ObjectId(deployment_id)],
            )
        )

        feature_table_id_2 = ObjectId()
        await app_container.feature_materialize_run_service.create_document(
            FeatureMaterializeRun(
                offline_store_feature_table_id=feature_table_id_2,
                offline_store_feature_table_name="device",
                scheduled_job_ts=datetime(2024, 7, 15, 10, 15, 0),
                completion_ts=datetime(2024, 7, 15, 10, 45, 0),
                completion_status="success",
                duration_from_scheduled_seconds=30,
                deployment_ids=[ObjectId(deployment_id)],
            )
        )

        response = test_api_client.get(
            f"{self.base_route}/{deployment_id}/job_history",
        )
        assert response.status_code == HTTPStatus.OK, response.content
        assert response.json() == {
            "feature_table_history": [
                {
                    "feature_table_id": str(feature_table_id_1),
                    "feature_table_name": "customer",
                    "runs": [
                        {
                            "scheduled_ts": "2024-07-15T10:10:00",
                            "completion_ts": "2024-07-15T10:25:00",
                            "completion_status": "success",
                            "duration_seconds": 15,
                            "incomplete_tile_tasks_count": 0,
                        },
                        {
                            "scheduled_ts": "2024-07-15T10:00:00",
                            "completion_ts": "2024-07-15T10:10:00",
                            "completion_status": "success",
                            "duration_seconds": 10,
                            "incomplete_tile_tasks_count": 0,
                        },
                    ],
                },
                {
                    "feature_table_id": str(feature_table_id_2),
                    "feature_table_name": "device",
                    "runs": [
                        {
                            "scheduled_ts": "2024-07-15T10:15:00",
                            "completion_ts": "2024-07-15T10:45:00",
                            "completion_status": "success",
                            "duration_seconds": 30,
                            "incomplete_tile_tasks_count": 0,
                        }
                    ],
                },
            ]
        }
