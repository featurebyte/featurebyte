"""
Tests for FeatureList route
"""

import os
import textwrap
from collections import OrderedDict, defaultdict
from http import HTTPStatus
from unittest.mock import AsyncMock, Mock, call, patch

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

from featurebyte.common.model_util import get_version
from featurebyte.common.utils import dataframe_from_json
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDetails
from featurebyte.query_graph.model.graph import QueryGraphModel
from featurebyte.schema.feature import FeatureCreate
from featurebyte.session.snowflake import SnowflakeSession
from tests.unit.common.test_utils import create_feature_list_batch_feature_create
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestFeatureListApi(BaseCatalogApiTestSuite):
    """
    TestFeatureListApi class
    """

    class_name = "FeatureList"
    base_route = "/feature_list"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_single.json"
    )
    payload_multi = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_list_multi.json"
    )
    object_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'FeatureList (id: "{payload["_id"]}") already exists. '
            'Get the existing object by `FeatureList.get(name="sf_feature_list")`.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "_id": object_id, "name": "random_name", "feature_ids": [object_id]},
            f'Feature (id: "{object_id}") not found. ' "Please save the Feature object first.",
        ),
        (
            payload_multi,
            f'Feature (id: "{payload_multi["feature_ids"][1]}") not found. Please save the Feature object first.',
        ),
        (
            {**payload, "feature_ids": []},
            [
                {
                    "input": [],
                    "loc": ["body", "FeatureListCreate", "feature_ids"],
                    "msg": "List should have at least 1 item after validation, not 0",
                    "type": "too_short",
                    "ctx": {"actual_length": 0, "field_type": "List", "min_length": 1},
                },
                {
                    "input": {**payload, "feature_ids": []},
                    "loc": ["body", "FeatureListNewVersionCreate", "source_feature_list_id"],
                    "msg": "Field required",
                    "type": "missing",
                },
                {
                    "input": {**payload, "feature_ids": []},
                    "loc": ["body", "FeatureListNewVersionCreate", "features"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ],
        ),
    ]

    @pytest.fixture(name="mock_snowflake_session")
    def mock_get_session_return_snowflake_session(self, mock_get_session):
        """Mock get_session to return a SnowflakeSession object"""
        mock_get_session.return_value = SnowflakeSession(
            account="test_account",
            warehouse="test_warehouse",
            database_name="test_database",
            schema_name="test_schema",
            role_name="TESTING",
            database_credential={
                "type": "USERNAME_PASSWORD",
                "username": "test_username",
                "password": "test_password",
            },
        )
        yield mock_get_session

    @pytest.fixture(autouse=True)
    def always_patched_observation_table_service(
        self, patched_observation_table_service_for_preview, mock_deployment_flow
    ):
        """
        Patch ObservationTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_observation_table_service_for_preview, mock_deployment_flow

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("context", "context"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()

            if api_object.endswith("_table"):
                # tag table entity for table objects
                self.tag_table_entity(api_client, api_object, payload)

    @staticmethod
    def _save_a_new_feature_version(api_client, feature_id, time_modulo_frequency=None):
        """Save a new version of a feature"""
        feature_response = api_client.post(
            "/feature",
            json={
                "source_feature_id": feature_id,
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
                        "feature_job_setting": {
                            "blind_spot": "1d",
                            "period": "1d",
                            "offset": time_modulo_frequency or "12h",
                        },
                    }
                ],
            },
        )
        assert feature_response.status_code == HTTPStatus.CREATED, feature_response.text
        return feature_response

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        for i in range(3):
            # make a new feature from feature_sum_30m & create a new feature_ids
            response = self._save_a_new_feature_version(
                api_client, feature_payload["_id"], f"{i + 1}h"
            )
            assert response.status_code == HTTPStatus.CREATED
            new_version_id = response.json()["_id"]

            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            payload["feature_ids"] = [new_version_id]
            yield payload

    @pytest.fixture(autouse=True)
    def mock_online_enable_service_update_data_warehouse(self):
        """Mock _update_data_warehouse method in OnlineEnableService to make it a no-op"""
        with patch("featurebyte.service.deploy.OnlineEnableService.update_data_warehouse"):
            yield

    @pytest.mark.asyncio
    async def test_create_201__with_existing_feature_list_namespace(
        self, test_api_client_persistent, create_success_response, user_id
    ):
        """Test create (success) - with existing feature list namespace"""
        _ = user_id
        assert create_success_response.status_code == HTTPStatus.CREATED
        result = create_success_response.json()

        test_api_client, _ = test_api_client_persistent
        # create a new feature
        feature_response = self._save_a_new_feature_version(
            test_api_client, result["feature_ids"][0]
        )
        feature_id = feature_response.json()["_id"]
        response = test_api_client.patch(
            f"/feature/{feature_id}",
            json={"readiness": "PRODUCTION_READY", "ignore_guardrails": True},
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # create a new feature list version
        response = test_api_client.post(
            self.base_route, json={"source_feature_list_id": result["_id"], "features": []}
        )
        expected_readiness_dist = [{"count": 1, "readiness": "PRODUCTION_READY"}]
        new_fl_dict = response.json()
        assert new_fl_dict["readiness_distribution"] == expected_readiness_dist
        assert new_fl_dict["feature_list_namespace_id"] == result["feature_list_namespace_id"]

        # check feature list namespace
        namespace_response = test_api_client.get(
            f"/feature_list_namespace/{result['feature_list_namespace_id']}"
        )
        namespace_response_dict = namespace_response.json()
        assert namespace_response_dict["feature_list_ids"] == [result["_id"], new_fl_dict["_id"]]
        assert namespace_response_dict["default_feature_list_id"] == new_fl_dict["_id"]

    def test_create_201_multiple_features(self, test_api_client_persistent, user_id):
        """Create feature list with multiple features"""
        _ = user_id
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # save another feature
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        response = test_api_client.post("/feature", json=feature_payload)
        assert response.status_code == HTTPStatus.CREATED

        # make sure the payload feature_ids is in non-sorted order
        payload_multi = self.payload_multi.copy()
        payload_multi["feature_ids"] = list(reversed(payload_multi["feature_ids"]))
        assert payload_multi["feature_ids"] != sorted(payload_multi["feature_ids"])

        # check that feature_ids in post response are same order as in payload
        response = test_api_client.post("/feature_list", json=payload_multi)
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["feature_ids"] == payload_multi["feature_ids"]

    @pytest.fixture(name="new_feature_list_version_response")
    def new_feature_list_version_response_fixture(
        self, test_api_client_persistent, create_success_response
    ):
        """New feature list version response"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()

        # create a new feature version
        feature_id = create_response_dict["feature_ids"][0]
        feature_response = self._save_a_new_feature_version(test_api_client, feature_id)
        feature_response_dict = feature_response.json()

        # then create a new feature list version
        response = test_api_client.post(
            self.base_route,
            json={"source_feature_list_id": create_response_dict["_id"], "features": []},
        )
        assert response.json()["feature_ids"] == [feature_response_dict["_id"]]
        return response

    def test_create_201__create_new_version(
        self, create_success_response, new_feature_list_version_response
    ):
        """Test create new version (success)"""
        create_response_dict = create_success_response.json()
        response = new_feature_list_version_response
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED
        assert response_dict["version"] == {"name": get_version(), "suffix": 1}
        assert (
            response_dict["feature_list_namespace_id"]
            == create_response_dict["feature_list_namespace_id"]
        )

    def test_create_422__different_feature_stores(self, test_api_client_persistent):
        """
        Test feature list with different feature stores
        """
        test_api_client, _ = test_api_client_persistent
        # create feature_store, event_table & feature
        self.setup_creation_route(api_client=test_api_client)

        # create another feature_store, event_table & feature with different feature_store
        feature_store = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        feature_store["_id"] = str(ObjectId())
        feature_store["name"] = f'new_{feature_store["name"]}'
        feature_store["details"] = {
            key: f"{value}_1" for key, value in feature_store["details"].items()
        }

        event_table = self.load_payload("tests/fixtures/request_payloads/event_table.json")
        event_table["_id"] = str(ObjectId())
        event_table["name"] = f'new_{event_table["name"]}'
        tabular_source = {
            "feature_store_id": feature_store["_id"],
            "table_details": event_table["tabular_source"]["table_details"],
        }
        event_table["tabular_source"] = tabular_source

        feature = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature["tabular_source"] = tabular_source

        payload_api_object_pairs = [
            (feature_store, "feature_store"),
            (event_table, "event_table"),
            (feature, "feature"),
        ]
        for payload, api_object in payload_api_object_pairs:
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

        # test feature list post route
        response = test_api_client.post(f"{self.base_route}", json=self.payload_multi)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == (
            "All the Feature objects within the same FeatureList object must be from the same feature store."
        )

    def test_create_422__duplicated_feature_name(self, test_api_client_persistent):
        """
        Test feature list with different feature stores
        """
        test_api_client, _ = test_api_client_persistent
        # create feature_store, event_table & feature
        self.setup_creation_route(api_client=test_api_client)

        # create another feature with the same name
        feature_dict = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        response = self._save_a_new_feature_version(test_api_client, feature_dict["_id"])
        assert response.status_code == HTTPStatus.CREATED, response.text
        new_feature_id = response.json()["_id"]

        payload = self.load_payload("tests/fixtures/request_payloads/feature_list_single.json")
        payload["feature_ids"].append(new_feature_id)

        # check that feature_ids in post response are sorted
        response = test_api_client.post("/feature_list", json=payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.text
        assert response.json()["detail"] == (
            "Two Feature objects must not share the same name in a FeatureList object."
        )

    def test_create_422__create_new_version(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create new version (unprocessable entity)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()

        # test create a new version with no change
        response = test_api_client.post(
            self.base_route,
            json={"source_feature_list_id": create_response_dict["_id"], "features": []},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "No change detected on the new feature list version."

        # test create a new version with duplicated feature name
        payload = {
            "source_feature_list_id": create_response_dict["_id"],
            "features": [
                {"name": "dup_feat_name", "version": "V230330"},
                {"name": "dup_feat_name", "version": "V230330"},
            ],
        }
        response = test_api_client.post(
            self.base_route,
            json=payload,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "input": payload,
                "loc": ["body", "FeatureListCreate", "name"],
                "msg": "Field required",
                "type": "missing",
            },
            {
                "input": payload,
                "loc": ["body", "FeatureListCreate", "feature_ids"],
                "msg": "Field required",
                "type": "missing",
            },
            {
                "ctx": {"error": {}},
                "input": payload["features"],
                "loc": ["body", "FeatureListNewVersionCreate", "features"],
                "msg": 'Value error, Name "dup_feat_name" is duplicated (field: name).',
                "type": "value_error",
            },
        ]

        # test create a new version with allow_unchanged_feature_list_version
        response = test_api_client.post(
            self.base_route,
            json={
                "source_feature_list_id": create_response_dict["_id"],
                "features": [],
                "allow_unchanged_feature_list_version": True,
            },
        )
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["_id"] == create_response_dict["_id"]

    def test_list_200__filter_by_name_and_version(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list (success) when filtering by name and version"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_multiple_success_responses[0].json()

        # create a new feature version
        feature_id = create_response_dict["feature_ids"][0]
        feature_response = self._save_a_new_feature_version(test_api_client, feature_id)
        assert feature_response.status_code == HTTPStatus.CREATED

        # then create a new feature list version
        new_version_response = test_api_client.post(
            self.base_route,
            json={"source_feature_list_id": create_response_dict["_id"], "features": []},
        )

        # check retrieving old feature list version
        version = create_response_dict["version"]["name"]
        response = test_api_client.get(
            self.base_route, params={"name": create_response_dict["name"], "version": version}
        )
        response_dict = response.json()
        create_response_dict["is_default"] = False
        create_response_dict.pop("feature_clusters")  # feature_clusters is not returned
        assert response_dict["total"] == 1
        assert response_dict["data"] == [create_response_dict]

        # check retrieving new feature list version
        response = test_api_client.get(
            self.base_route,
            params={"name": create_response_dict["name"], "version": f"{version}_1"},
        )
        response_dict = response.json()
        new_version_response_dict = new_version_response.json()
        new_version_response_dict.pop("feature_clusters")  # feature_clusters is not returned
        assert response_dict["total"] == 1
        assert response_dict["data"] == [new_version_response_dict]

    def test_list_200__filter_by_namespace_id(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list (filtered by feature list namespace id)"""
        test_api_client, _ = test_api_client_persistent
        namespace_map = defaultdict(set)
        for success_response in create_multiple_success_responses:
            response_dict = success_response.json()
            namespace_map[response_dict["feature_list_namespace_id"]].add(response_dict["_id"])

        for namespace_id, ids in namespace_map.items():
            filter_response = test_api_client.get(
                self.base_route, params={"feature_list_namespace_id": namespace_id}
            )
            filter_response_dict = filter_response.json()
            assert filter_response_dict["total"] == len(ids)
            response_ids = set(item["_id"] for item in filter_response_dict["data"])
            assert response_ids == ids

        # test negative cases
        negative_response = test_api_client.get(
            self.base_route, params={"feature_list_namespace_id": str(ObjectId())}
        )
        assert negative_response.json()["total"] == 0, negative_response.json()

    def test_update_200__deploy_with_make_production_ready(
        self, test_api_client_persistent, create_success_response
    ):
        """Test update (success) with make production ready"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # deploy the feature list
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"make_production_ready": True}
        )
        assert response.status_code == HTTPStatus.ACCEPTED

        response = test_api_client.post("/deployment", json={"feature_list_id": doc_id})
        deployment_id = response.json()["payload"]["output_document_id"]
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["status"] == "SUCCESS"

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["deployed"] is False

        # enable the deployment
        response = test_api_client.patch(f"/deployment/{deployment_id}", json={"enabled": True})
        assert response.status_code == HTTPStatus.ACCEPTED
        assert response.json()["status"] == "SUCCESS"

        # check serving endpoint populated in info
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        version = get_version()
        expected_info_response = {
            "name": "sf_feature_list",
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "grocery"}
            ],
            "version_count": 1,
            "dtype_distribution": [{"dtype": "FLOAT", "count": 1}],
            "status": "DEPLOYED",
            "feature_count": 1,
            "version": {"this": version, "default": version},
            "production_ready_fraction": {"this": 1.0, "default": 1.0},
            "versions_info": None,
            "deployed": True,
            "catalog_name": "grocery",
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict

        # disable deployment
        response = test_api_client.patch(f"/deployment/{deployment_id}", json={"enabled": False})
        assert response.status_code == HTTPStatus.ACCEPTED
        assert response.json()["status"] == "SUCCESS"

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["deployed"] is False

        # test feature list delete
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"].startswith(
            "FeatureList is referenced by Deployment: Deployment with sf_feature_list_"
        ), response.json()

    def test_delete_204(self, test_api_client_persistent, create_success_response):
        """Test delete (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        namespace_id = create_response_dict["feature_list_namespace_id"]

        namespace_response = test_api_client.get(f"/feature_list_namespace/{namespace_id}")
        assert namespace_response.status_code == HTTPStatus.OK
        assert namespace_response.json()["feature_list_ids"] == [doc_id]

        # delete feature list
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK

        # check that feature list and feature list namespace are deleted
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

        response = test_api_client.get(f"/feature_list_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_204__namespace_not_deleted(
        self, test_api_client_persistent, create_success_response, new_feature_list_version_response
    ):
        """Test delete (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        new_doc_id = new_feature_list_version_response.json()["_id"]
        namespace_id = create_response_dict["feature_list_namespace_id"]

        # check namespace before delete
        namespace_dict = test_api_client.get(f"/feature_list_namespace/{namespace_id}").json()
        assert namespace_dict["feature_list_ids"] == [doc_id, new_doc_id]

        # delete feature list
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK

        # check namespace after delete
        namespace_dict = test_api_client.get(f"/feature_list_namespace/{namespace_id}").json()
        assert namespace_dict["feature_list_ids"] == [new_doc_id]

    def test_delete_422__non_draft_feature_list(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete (unprocessible entity)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        namespace_id = create_response_dict["feature_list_namespace_id"]

        # change status to public draft
        response = test_api_client.patch(
            f"/feature_list_namespace/{namespace_id}", json={"status": "PUBLIC_DRAFT"}
        )
        assert response.status_code == HTTPStatus.OK

        # check that feature list cannot be deleted
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        expected_error = "Only feature list with DRAFT status can be deleted."
        assert response.json()["detail"] == expected_error

        # check that feature list is not deleted
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK

    @pytest.mark.asyncio
    async def test_get_info_200(self, test_api_client_persistent, create_success_response):
        """Test retrieve info"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        version = get_version()
        expected_info_response = {
            "name": "sf_feature_list",
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "grocery"}
            ],
            "dtype_distribution": [{"count": 1, "dtype": "FLOAT"}],
            "version_count": 1,
            "feature_count": 1,
            "version": {"this": version, "default": version},
            "production_ready_fraction": {"this": 0, "default": 0},
            "deployed": False,
            "catalog_name": "grocery",
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict
        assert "created_at" in response_dict
        assert response_dict["versions_info"] is None

        verbose_response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": True}
        )
        assert response.status_code == HTTPStatus.OK, response.text
        verbose_response_dict = verbose_response.json()
        assert verbose_response_dict.items() > expected_info_response.items(), verbose_response.text
        assert "created_at" in verbose_response_dict
        assert verbose_response_dict["versions_info"] is not None

    @pytest.fixture(name="featurelist_feature_clusters")
    def featurelist_feature_clusters_fixture(
        self, create_success_response, test_api_client_persistent
    ):
        """
        featurelist_preview_payload fixture
        """
        test_api_client, _ = test_api_client_persistent
        featurelist = create_success_response.json()

        feature_id = featurelist["feature_ids"][0]
        response = test_api_client.get(f"/feature/{feature_id}")
        assert response.status_code == HTTPStatus.OK
        feature = response.json()

        feature_store_id = feature["tabular_source"]["feature_store_id"]

        return [
            {
                "feature_store_id": feature_store_id,
                "graph": feature["graph"],
                "node_names": [feature["node_name"]],
                "feature_node_relationships_infos": [
                    {
                        "node_name": "project_1",
                        "relationships_info": [],
                        "primary_entity_ids": feature["primary_entity_ids"],
                    }
                ],
                "combined_relationships_info": [],
                "feature_node_definition_hashes": [
                    {
                        "feature_id": feature_id,
                        "feature_name": feature["name"],
                        "node_name": "project_1",
                        "definition_hash": "e08e2ffd6a5817b174d075895b17ee3a3bb9df38",
                    }
                ],
            }
        ]

    @pytest.fixture(name="featurelist_preview_payload")
    def featurelist_preview_payload_fixture(self, featurelist_feature_clusters):
        """
        featurelist_preview_payload fixture
        """
        return {
            "feature_clusters": featurelist_feature_clusters,
            "point_in_time_and_serving_name_list": [
                {
                    "cust_id": "C1",
                    "POINT_IN_TIME": "2022-04-01",
                }
            ],
        }

    def test_preview_200(
        self,
        test_api_client_persistent,
        featurelist_preview_payload,
        mock_get_session,
    ):
        """Test feature list preview"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using feature clusters
        response = test_api_client.post(
            f"{self.base_route}/preview", json=featurelist_preview_payload
        )
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_using_feature_list_id_200(
        self,
        test_api_client_persistent,
        create_success_response,
        featurelist_preview_payload,
        mock_get_session,
    ):
        """Test feature list preview"""
        test_api_client, _ = test_api_client_persistent
        featurelist = create_success_response.json()
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using feature list id
        featurelist_preview_payload.pop("feature_clusters")
        featurelist_preview_payload["feature_list_id"] = featurelist["_id"]
        response = test_api_client.post(
            f"{self.base_route}/preview", json=featurelist_preview_payload
        )
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_using_observation_table_200(
        self,
        test_api_client_persistent,
        featurelist_preview_payload,
        mock_get_session,
    ):
        """Test feature list preview"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.list_table_schema.return_value = OrderedDict({
            "cust_id": ColumnSpecWithDetails(
                name="cust_id",
                dtype=DBVarType.INT,
                description=None,
            ),
            "POINT_IN_TIME": ColumnSpecWithDetails(
                name="POINT_IN_TIME",
                dtype=DBVarType.TIMESTAMP,
                description=None,
            ),
        })
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using observation table
        payload = self.load_payload("tests/fixtures/request_payloads/observation_table.json")
        response = test_api_client.post("/observation_table", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        response = self.wait_for_results(test_api_client, response)
        assert response.json()["status"] == "SUCCESS", response.json()["traceback"]
        obs_table_df = pd.DataFrame({
            "POINT_IN_TIME": pd.to_datetime(["2022-04-01"]),
            "cust_id": ["C1"],
        })
        mock_session.execute_query.side_effect = (obs_table_df, expected_df)

        featurelist_preview_payload.pop("point_in_time_and_serving_name_list")
        featurelist_preview_payload["observation_table_id"] = "646f6c1c0ed28a5271fb02d7"
        response = test_api_client.post(
            f"{self.base_route}/preview", json=featurelist_preview_payload
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_feature_list_preview__exceed_max_feature_limit(
        self,
        test_api_client_persistent,
        featurelist_preview_payload,
    ):
        """Test feature list preview with too many features"""
        with patch.dict(os.environ, {"FEATUREBYTE_FEATURE_LIST_PREVIEW_MAX_FEATURE_NUM": "0"}):
            test_api_client, _ = test_api_client_persistent
            response = test_api_client.post(
                f"{self.base_route}/preview", json=featurelist_preview_payload
            )
            assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
            assert response.json()["detail"] == "Feature list preview must have 0 features or less"

    @pytest.fixture(name="featurelist_get_historical_features_payload")
    def featurelist_get_historical_features_payload_fixture(self, featurelist_feature_clusters):
        """
        featurelist_get_historical_features_payload fixture
        """
        return {
            "feature_clusters": featurelist_feature_clusters,
            "serving_names_mapping": {},
        }

    def test_sql_200(self, test_api_client_persistent, featurelist_preview_payload):
        """Test featurelist sql (success)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(f"{self.base_route}/sql", json=featurelist_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert response.json().endswith(
            textwrap.dedent(
                """
                SELECT
                  CAST("_fb_internal_cust_id_window_w1800_sum_e8c51d7d1ec78e1f35195fc0cf61221b3f830295" AS DOUBLE) AS "sum_30m"
                FROM _FB_AGGREGATED AS AGG
                """
            ).strip()
        )

    def test_feature_clusters_derived_and_stored(
        self, create_success_response, featurelist_feature_clusters
    ):
        """Test feature_clusters field is derived and stored"""
        feature_clusters = create_success_response.json()["feature_clusters"]
        assert isinstance(feature_clusters, list)
        assert len(feature_clusters) == 1
        expected_feature_cluster = featurelist_feature_clusters[0]
        graph = QueryGraphModel(**expected_feature_cluster["graph"])
        groupby_node = graph.get_node_by_name("groupby_1").parameters.model_dump()
        groupby_node["names"] = ["sum_30m"]
        groupby_node["windows"] = ["30m"]
        assert feature_clusters[0] == expected_feature_cluster

    @freeze_time("2022-01-02 10:00:00")
    def test_get_feature_job_logs_200(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test get feature job logs"""
        test_api_client, _ = test_api_client_persistent
        featurelist = create_success_response.json()
        feature_list_id = featurelist["_id"]

        # use the feature payload to get the aggregation id
        feature_id = featurelist["feature_ids"][0]
        res = test_api_client.get(f"/feature/{feature_id}")
        assert res.status_code == HTTPStatus.OK
        graph = QueryGraphModel(**res.json()["graph"])
        groupby_node = graph.get_node_by_name("groupby_1")
        aggregation_id = groupby_node.parameters.aggregation_id

        job_logs = pd.DataFrame({
            "SESSION_ID": ["SID1"] * 4 + ["SID2"] * 2,
            "AGGREGATION_ID": [aggregation_id] * 6,
            "CREATED_AT": pd.to_datetime([
                "2020-01-02 18:00:00",
                "2020-01-02 18:01:00",
                "2020-01-02 18:02:00",
                "2020-01-02 18:03:00",
                "2020-01-02 18:00:00",
                "2020-01-02 18:05:00",
            ]),
            "STATUS": [
                "STARTED",
                "MONITORED",
                "GENERATED",
                "COMPLETED",
                "STARTED",
                "GENERATED_FAILED",
            ],
            "MESSAGE": [""] * 5 + ["Some error has occurred"],
        })
        with patch(
            "featurebyte.service.tile_job_log.TileJobLogService.get_logs_dataframe"
        ) as mock_get_jobs_dataframe:
            mock_get_jobs_dataframe.return_value = job_logs
            response = test_api_client.get(f"{self.base_route}/{feature_list_id}/feature_job_logs")
        assert response.status_code == HTTPStatus.OK
        expected_df = pd.DataFrame({
            "SESSION_ID": ["SID1", "SID2"],
            "AGGREGATION_ID": [aggregation_id] * 2,
            "SCHEDULED": pd.to_datetime(["2020-01-02 17:35:00"] * 2),
            "STARTED": pd.to_datetime(["2020-01-02 18:00:00"] * 2),
            "COMPLETED": pd.to_datetime(["2020-01-02 18:03:00", pd.NaT]),
            "QUEUE_DURATION": [1500.0] * 2,
            "COMPUTE_DURATION": [180.0, np.nan],
            "TOTAL_DURATION": [1680.0, np.nan],
            "ERROR": [np.nan, "Some error has occurred"],
        })
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)
        assert mock_get_jobs_dataframe.call_args == call(
            aggregation_ids=[aggregation_id], hour_limit=24
        )

    @pytest.mark.asyncio
    async def test_feature_list_batch_feature_create__success(
        self, test_api_client_persistent, mock_snowflake_session
    ):
        """Test feature list batch feature create async task (success)"""
        _ = mock_snowflake_session
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # prepare feature list batch feature create payload
        payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature_create = FeatureCreate(**payload)
        feature_list_name, feature_list_id = "test_feature_list", ObjectId()
        feature_list_batch_feature_create = create_feature_list_batch_feature_create(
            features=[feature_create],
            feature_list_name=feature_list_name,
            feature_list_id=feature_list_id,
            conflict_resolution="raise",
        )

        # check feature is not created
        response = test_api_client.get(f"feature/{feature_create.id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

        # create batch feature create task
        task_response = test_api_client.post(
            f"{self.base_route}/batch", json=feature_list_batch_feature_create.json_dict()
        )
        response = self.wait_for_results(test_api_client, task_response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS"
        assert response_dict["output_path"] == f"/feature_list/{feature_list_id}"
        assert response_dict["traceback"] is None

        # check feature_list & feature is created
        response = test_api_client.get(f"{self.base_route}/{feature_list_id}")
        response_dict = response.json()
        assert response_dict["name"] == feature_list_name
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(f"feature/{feature_create.id}")
        response_dict = response.json()
        assert response_dict["name"] == feature_create.name
        assert response.status_code == HTTPStatus.OK

    @pytest.mark.asyncio
    @patch(
        "featurebyte.worker.util.batch_feature_creator.BatchFeatureCreator.is_generated_feature_consistent",
        new_callable=AsyncMock,
    )
    async def test_feature_list_batch_feature_create__failure(
        self,
        mock_is_generated_feature_consistent,
        test_api_client_persistent,
        mock_snowflake_session,
        user_id,
    ):
        """Test batch feature create async task"""
        _ = mock_snowflake_session
        mock_is_generated_feature_consistent.side_effect = [False, True]
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # prepare batch feature create payload
        payload1 = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        payload2 = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_create1 = FeatureCreate(**payload1)
        feature_create2 = FeatureCreate(**payload2)
        feature_list_name, feature_list_id = "test_feature_list", ObjectId()
        feature_list_batch_feature_create = create_feature_list_batch_feature_create(
            features=[feature_create1, feature_create2],
            feature_list_name=feature_list_name,
            feature_list_id=feature_list_id,
            conflict_resolution="raise",
        )

        # create feature list batch feature create task
        task_response = test_api_client.post(
            f"{self.base_route}/batch", json=feature_list_batch_feature_create.json_dict()
        )

        # check user id
        assert task_response.json()["payload"]["user_id"] == str(user_id)

        # retrieve task results
        response = self.wait_for_results(test_api_client, task_response)
        response_dict = response.json()
        expected_traceback = (
            "Inconsistent feature definitions detected: sum_2h\n"
            "The inconsistent features have been deleted."
        )
        assert expected_traceback in response_dict["traceback"]
        assert response_dict["status"] == "FAILURE"

        # check feature list & feature1 is not created
        response = test_api_client.get(f"{self.base_route}/{feature_list_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

        response = test_api_client.get(f"feature/{feature_create1.id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

        # check feature2 is created
        response = test_api_client.get(f"feature/{feature_create2.id}")
        assert response.status_code == HTTPStatus.OK

    def test_feature_list_batch_feature_create__success_when_skip_batch_feature_creation(
        self, test_api_client_persistent
    ):
        """Test batch feature create async task (success)"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        feature_list_id = str(ObjectId())

        payload = {
            "_id": feature_list_id,
            "name": "test_feature_list",
            "features": [
                {
                    "id": str(ObjectId()),
                    "name": "sum_30m",
                    "node_name": "non_exist_node",
                    "tabular_source": {
                        "feature_store_id": str(ObjectId()),
                        "table_details": {
                            "database_name": "db",
                            "schema_name": "schema",
                            "table_name": "table",
                        },
                    },
                },
            ],
            "graph": {"nodes": [], "edges": []},
            "conflict_resolution": "retrieve",
            "skip_batch_feature_creation": True,
        }
        task_response = test_api_client.post(f"{self.base_route}/batch", json=payload)
        assert task_response.status_code == HTTPStatus.CREATED
        assert task_response.json()["status"] == "SUCCESS"

        # retrieve feature list
        response = test_api_client.get(f"{self.base_route}/{feature_list_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["name"] == "test_feature_list"

    def test_feature_list_batch_feature_create__failure_when_skip_batch_feature_creation(
        self,
        test_api_client_persistent,
    ):
        """Test batch feature create async task (failure)"""
        test_api_client, _ = test_api_client_persistent

        unsaved_feature_id = str(ObjectId())
        payload = {
            "name": "test_feature_list",
            "features": [
                {
                    "id": unsaved_feature_id,
                    "name": "non_exist_feature",
                    "node_name": "non_exist_node",
                    "tabular_source": {
                        "feature_store_id": str(ObjectId()),
                        "table_details": {
                            "database_name": "db",
                            "schema_name": "schema",
                            "table_name": "table",
                        },
                    },
                },
            ],
            "graph": {"nodes": [], "edges": []},
            "conflict_resolution": "raise",
            "skip_batch_feature_creation": True,
        }
        task_response = test_api_client.post(f"{self.base_route}/batch", json=payload)
        assert task_response.status_code == HTTPStatus.CREATED
        assert task_response.json()["status"] == "FAILURE"
        traceback = task_response.json()["traceback"]
        expected_message = (
            f'Feature (id: "{unsaved_feature_id}") not found. Please save the Feature object first.'
        )
        assert expected_message in traceback

    def test_feature_list_creation_job(self, test_api_client_persistent):
        """Test feature list creation job"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create feature list
        feature_list_id = str(ObjectId())
        feat_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_list_create_payload = {
            "_id": feature_list_id,
            "name": "test_feature_list",
            "features": [{"name": feat_payload["name"], "id": feat_payload["_id"]}],
            "features_conflict_resolution": "raise",
        }
        task_response = test_api_client.post(
            f"{self.base_route}/job", json=feature_list_create_payload
        )
        self.wait_for_results(test_api_client, task_response)
        assert task_response.status_code == HTTPStatus.CREATED
        assert task_response.json()["status"] == "SUCCESS"

        feature_list_response = test_api_client.get(f"{self.base_route}/{feature_list_id}")
        feature_list_dict = feature_list_response.json()
        assert feature_list_response.status_code == HTTPStatus.OK
        assert feature_list_dict["name"] == "test_feature_list"
        assert feature_list_dict["feature_ids"] == [feat_payload["_id"]]

        # check when the feature is not found
        unknown_feature_id = str(ObjectId())
        feature_list_create_payload["features"][0]["id"] = unknown_feature_id
        task_response = test_api_client.post(
            f"{self.base_route}/job", json=feature_list_create_payload
        )
        expected_error = (
            f'Feature (id: "{unknown_feature_id}") not found. Please save the Feature object first.'
        )
        assert task_response.status_code == HTTPStatus.CREATED
        assert task_response.json()["status"] == "FAILURE"
        assert expected_error in task_response.json()["traceback"]

        # check resolve conflict
        another_test_feature_list_id = str(ObjectId())
        feature_list_create_payload["name"] = "another_test_feature_list"
        feature_list_create_payload["_id"] = another_test_feature_list_id
        feature_list_create_payload["features_conflict_resolution"] = "retrieve"
        task_response = test_api_client.post(
            f"{self.base_route}/job", json=feature_list_create_payload
        )
        assert task_response.status_code == HTTPStatus.CREATED
        assert task_response.json()["status"] == "SUCCESS"

        # check newly created feature list
        feature_list_response = test_api_client.get(
            f"{self.base_route}/{another_test_feature_list_id}"
        )
        feature_list_dict = feature_list_response.json()
        assert feature_list_response.status_code == HTTPStatus.OK
        assert feature_list_dict["name"] == "another_test_feature_list"
        assert feature_list_dict["feature_ids"] == [feat_payload["_id"]]

    def test_feature_list_creation_job__using_feature_ids_only(self, test_api_client_persistent):
        """Test feature list creation job using feature ids only"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # create feature list
        feature_list_id = str(ObjectId())
        feat_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_list_create_payload = {
            "_id": feature_list_id,
            "name": "test_feature_list",
            "features": [feat_payload["_id"]],
            "features_conflict_resolution": "raise",
        }
        task_response = test_api_client.post(
            f"{self.base_route}/job", json=feature_list_create_payload
        )
        self.wait_for_results(test_api_client, task_response)
        assert task_response.status_code == HTTPStatus.CREATED
        assert task_response.json()["status"] == "SUCCESS"

        feature_list_response = test_api_client.get(f"{self.base_route}/{feature_list_id}")
        feature_list_dict = feature_list_response.json()
        assert feature_list_response.status_code == HTTPStatus.OK
        assert feature_list_dict["name"] == "test_feature_list"
        assert feature_list_dict["feature_ids"] == [feat_payload["_id"]]

    def test_feature_list_creation_job__without_features(self, test_api_client_persistent):
        """Test feature list creation job using feature ids only"""
        test_api_client, _ = test_api_client_persistent

        feature_list_create_payload = {
            "_id": str(ObjectId()),
            "name": "test_feature_list",
            "features": [],
            "features_conflict_resolution": "raise",
        }
        task_response = test_api_client.post(
            f"{self.base_route}/job", json=feature_list_create_payload
        )
        assert task_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert (
            task_response.json()["detail"][0]["msg"]
            == "List should have at least 1 item after validation, not 0"
        )

    def test_request_sample_entity_serving_names(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
    ):
        """Test getting sample entity serving names for a feature list"""
        test_api_client, _ = test_api_client_persistent
        result = create_success_response.json()

        async def mock_execute_query(query):
            _ = query
            return pd.DataFrame([
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

        mock_session = mock_get_session.return_value
        mock_session.execute_query = mock_execute_query

        # Request sample entity serving names
        feature_list_id = result["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{feature_list_id}/sample_entity_serving_names?count=10",
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

    @pytest.mark.asyncio
    async def test_delete_feature_list_with_missing_remote_path(
        self,
        test_api_client_persistent,
        create_success_response,
        storage,
    ):
        """Test delete feature list with missing remote path"""
        test_api_client, _ = test_api_client_persistent
        feature_list_doc = create_success_response.json()
        feature_list_id = feature_list_doc["_id"]
        await storage.delete(feature_list_doc["feature_clusters_path"])
        with pytest.raises(FileNotFoundError):
            await storage.get_text(feature_list_doc["feature_clusters_path"])

        response = test_api_client.delete(f"{self.base_route}/{feature_list_id}")
        assert response.status_code == HTTPStatus.OK

        # check that feature list is deleted
        response = test_api_client.get(f"{self.base_route}/{feature_list_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND
