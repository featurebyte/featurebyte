"""
Tests for FeatureList route
"""
# pylint: disable=too-many-lines
import json
import textwrap
from collections import defaultdict
from http import HTTPStatus
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

from featurebyte.common.model_util import get_version
from featurebyte.common.utils import (
    dataframe_from_arrow_stream,
    dataframe_from_json,
    dataframe_to_arrow_bytes,
)
from featurebyte.enum import SourceType
from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.query_graph.model.graph import QueryGraphModel
from tests.unit.routes.base import BaseCatalogApiTestSuite


class TestFeatureListApi(BaseCatalogApiTestSuite):  # pylint: disable=too-many-public-methods
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
            'Feature (id: "642db61e28e1dffe39df3544") not found. Please save the Feature object first.',
        ),
        (
            {**payload, "feature_ids": []},
            [
                {
                    "loc": ["body", "feature_ids"],
                    "msg": "ensure this value has at least 1 items",
                    "type": "value_error.list.min_items",
                    "ctx": {"limit_value": 1},
                },
                {
                    "loc": ["body", "source_feature_list_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
                {
                    "loc": ["body", "features"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        ),
    ]

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_table", "event_table"),
            ("feature", "feature_sum_30m"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", headers={"active-catalog-id": str(catalog_id)}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED, response.json()

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        for i in range(3):
            # make a new feature from feature_sum_30m & create a new feature_ids
            feature_payload = self.load_payload(
                "tests/fixtures/request_payloads/feature_sum_30m.json"
            )
            new_feature_id = str(ObjectId())
            response = api_client.post(
                "/feature",
                json={**feature_payload, "_id": new_feature_id},
            )
            assert response.status_code == HTTPStatus.CREATED

            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            payload["feature_ids"] = [new_feature_id]
            payload["feature_list_namespace_id"] = str(ObjectId())
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
        assert create_success_response.status_code == HTTPStatus.CREATED
        result = create_success_response.json()

        test_api_client, persistent = test_api_client_persistent
        # create a new feature
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        feature_payload["version"] = {"name": get_version(), "suffix": 1}
        feature_id = await persistent.insert_one(
            collection_name="feature",
            document={
                **feature_payload,
                "_id": ObjectId(),
                "user_id": ObjectId(user_id),
                "readiness": "PRODUCTION_READY",
                "catalog_id": DEFAULT_CATALOG_ID,
            },
            user_id=user_id,
        )

        # prepare a new payload with existing feature list namespace
        new_payload = self.payload.copy()
        new_payload["_id"] = str(ObjectId())
        new_payload["feature_ids"] = [str(feature_id)]
        new_payload["feature_list_namespace_id"] = result["feature_list_namespace_id"]
        expected_readiness_dist = [{"count": 1, "readiness": "PRODUCTION_READY"}]
        response = test_api_client.post(f"{self.base_route}", json=new_payload)
        new_fl_dict = response.json()
        assert new_fl_dict["readiness_distribution"] == expected_readiness_dist
        assert new_fl_dict["feature_list_namespace_id"] == result["feature_list_namespace_id"]

        # check feature list namespace
        namespace_response = test_api_client.get(
            f"/feature_list_namespace/{result['feature_list_namespace_id']}"
        )
        namespace_response_dict = namespace_response.json()
        assert namespace_response_dict["feature_list_ids"] == [result["_id"], new_fl_dict["_id"]]
        assert namespace_response_dict["readiness_distribution"] == expected_readiness_dist
        assert namespace_response_dict["default_version_mode"] == "AUTO"
        assert namespace_response_dict["default_feature_list_id"] == new_fl_dict["_id"]

    def test_create_201_multiple_features(self, test_api_client_persistent, user_id):
        """Create feature list with multiple features"""
        _ = user_id
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # save another feature
        payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        response = test_api_client.post("/feature", json=payload)
        assert response.status_code == HTTPStatus.CREATED

        # make sure the payload feature_ids is in non-sorted order
        payload_multi = self.payload_multi.copy()
        payload_multi["feature_ids"] = list(reversed(payload_multi["feature_ids"]))
        assert payload_multi["feature_ids"] != sorted(payload_multi["feature_ids"])

        # check that feature_ids in post response are sorted
        response = test_api_client.post("/feature_list", json=payload_multi)
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["feature_ids"] == sorted(payload_multi["feature_ids"])

    @pytest.fixture(name="new_feature_list_version_response")
    def new_feature_list_version_response_fixture(
        self, test_api_client_persistent, create_success_response
    ):
        """New feature list version response"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()

        # create a new feature version
        feature_id = create_response_dict["feature_ids"][0]
        feature_response = test_api_client.post(
            "/feature",
            json={
                "source_feature_id": feature_id,
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
                        "feature_job_setting": {
                            "blind_spot": "1d",
                            "frequency": "1d",
                            "time_modulo_frequency": "1h",
                        },
                    }
                ],
            },
        )
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
        feature_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
        new_feature_id = str(ObjectId())
        feature_payload["_id"] = new_feature_id
        response = test_api_client.post("/feature", json=feature_payload)
        assert response.status_code == HTTPStatus.CREATED, response.text

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
        response = test_api_client.post(
            self.base_route,
            json={
                "source_feature_list_id": create_response_dict["_id"],
                "features": [
                    {"name": "dup_feat_name", "version": "V230330"},
                    {"name": "dup_feat_name", "version": "V230330"},
                ],
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {"loc": ["body", "name"], "msg": "field required", "type": "value_error.missing"},
            {
                "loc": ["body", "feature_ids"],
                "msg": "field required",
                "type": "value_error.missing",
            },
            {
                "loc": ["body", "features"],
                "msg": 'Name "dup_feat_name" is duplicated (field: name).',
                "type": "value_error",
            },
        ]

    def test_list_200__filter_by_name_and_version(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list (success) when filtering by name and version"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_multiple_success_responses[0].json()

        # create a new feature version
        feature_id = create_response_dict["feature_ids"][0]
        feature_response = test_api_client.post(
            "/feature",
            json={
                "source_feature_id": feature_id,
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
                        "feature_job_setting": {
                            "blind_spot": "1d",
                            "frequency": "1d",
                            "time_modulo_frequency": "1h",
                        },
                    }
                ],
            },
        )
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
        assert response_dict["total"] == 1
        assert response_dict["data"] == [create_response_dict]

        # check retrieving new feature list version
        response = test_api_client.get(
            self.base_route,
            params={"name": create_response_dict["name"], "version": f"{version}_1"},
        )
        response_dict = response.json()
        assert response_dict["total"] == 1
        assert response_dict["data"] == [new_version_response.json()]

    def _make_production_ready_and_deploy(self, client, feature_list_doc):
        doc_id = feature_list_doc["_id"]
        for feature_id in feature_list_doc["feature_ids"]:
            # upgrade readiness level to production ready first
            response = client.patch(
                f"/feature/{feature_id}", json={"readiness": "PRODUCTION_READY"}
            )
            assert response.status_code == HTTPStatus.OK

        response = client.patch(f"{self.base_route}/{doc_id}", json={})
        assert response.status_code == HTTPStatus.OK
        assert response.json()["deployed"] is False

        # deploy the feature list
        response = client.post("/deployment", json={"feature_list_id": doc_id})
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["status"] == "SUCCESS"

        response = client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["deployed"] is True

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
        self, test_api_client_persistent, create_success_response, api_object_to_id
    ):
        """Test update (success) with make production ready"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # deploy the feature list
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"make_production_ready": True}
        )
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.post("/deployment", json={"feature_list_id": doc_id})
        deployment_id = response.json()["payload"]["output_document_id"]
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["status"] == "SUCCESS"

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["deployed"] is True

        # check serving endpoint populated in info
        response = test_api_client.get(
            f"{self.base_route}/{doc_id}/info", params={"verbose": False}
        )
        version = get_version()
        feature_list_id = api_object_to_id["feature_list_single"]
        expected_info_response = {
            "name": "sf_feature_list",
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "default"}
            ],
            "default_version_mode": "AUTO",
            "version_count": 1,
            "dtype_distribution": [{"dtype": "FLOAT", "count": 1}],
            "status": "DEPLOYED",
            "feature_count": 1,
            "version": {"this": version, "default": version},
            "production_ready_fraction": {"this": 1.0, "default": 1.0},
            "versions_info": None,
            "deployed": True,
            "serving_endpoint": f"/feature_list/{feature_list_id}/online_features",
            "catalog_name": "default",
        }
        assert response.status_code == HTTPStatus.OK, response.text
        response_dict = response.json()
        assert response_dict.items() > expected_info_response.items(), response_dict

        # disable deployment
        response = test_api_client.patch(f"/deployment/{deployment_id}", json={"enabled": False})
        assert response.status_code == HTTPStatus.OK
        assert response.json()["status"] == "SUCCESS"

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert response.json()["deployed"] is False

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
        assert response.status_code == HTTPStatus.NO_CONTENT

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
        assert response.status_code == HTTPStatus.NO_CONTENT

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

    def test_delete_422__manual_mode_default_feature_list(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete (unprocessible entity)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        namespace_id = create_response_dict["feature_list_namespace_id"]

        # set feature list namespace default version mode to manual first
        response = test_api_client.patch(
            f"/feature_list_namespace/{namespace_id}", json={"default_version_mode": "MANUAL"}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["default_version_mode"] == "MANUAL"
        assert response_dict["default_feature_list_id"] == doc_id

        # check that the feature list cannot be deleted
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        expected_error = (
            "Feature list is the default feature list of the feature list namespace and the default version "
            "mode is manual. Please set another feature list as the default feature list or "
            "change the default version mode to auto."
        )
        assert response.json()["detail"] == expected_error

        # check that the feature list is not deleted
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
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "default"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "default"}
            ],
            "default_version_mode": "AUTO",
            "dtype_distribution": [{"count": 1, "dtype": "FLOAT"}],
            "version_count": 1,
            "feature_count": 1,
            "version": {"this": version, "default": version},
            "production_ready_fraction": {"this": 0, "default": 0},
            "deployed": False,
            "serving_endpoint": None,
            "catalog_name": "default",
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
        self, test_api_client_persistent, featurelist_preview_payload, mock_get_session
    ):
        """Test feature list preview"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")
        response = test_api_client.post(
            f"{self.base_route}/preview", json=featurelist_preview_payload
        )
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    @pytest.fixture(name="featurelist_get_historical_features_payload")
    def featurelist_get_historical_features_payload_fixture(self, featurelist_feature_clusters):
        """
        featurelist_get_historical_features_payload fixture
        """
        return {
            "feature_clusters": featurelist_feature_clusters,
            "serving_names_mapping": {},
        }

    def test_get_historical_features_200(
        self,
        test_api_client_persistent,
        featurelist_get_historical_features_payload,
        mock_get_session,
    ):
        """Test feature list get_historical_features"""
        test_api_client, _ = test_api_client_persistent
        observation_set = pd.DataFrame({"cust_id": [0, 1, 2], "POINT_IN_TIME": ["2022-04-01"] * 3})
        expected_df = pd.DataFrame({"a": [0, 1, 2]})

        async def mock_get_async_query_stream(query):
            _ = query
            yield dataframe_to_arrow_bytes(expected_df)

        expected_df = pd.DataFrame({"a": [0, 1, 2]})

        mock_session = mock_get_session.return_value
        mock_session.get_async_query_stream = mock_get_async_query_stream
        mock_session.source_type = SourceType.SNOWFLAKE
        mock_session.generate_session_unique_id = Mock(return_value="1")

        with patch("featurebyte.sql.tile_registry.TileRegistry.execute") as _:
            response = test_api_client.post(
                f"{self.base_route}/historical_features",
                data={"payload": json.dumps(featurelist_get_historical_features_payload)},
                files={"observation_set": dataframe_to_arrow_bytes(observation_set)},
                stream=True,
            )
            assert response.status_code == HTTPStatus.OK, response.json()

        # test streaming download works
        content = b""
        for chunk in response.iter_content(chunk_size=8192):
            content += chunk

        df = dataframe_from_arrow_stream(content)
        assert_frame_equal(df, expected_df)

    def test_sql_200(self, test_api_client_persistent, featurelist_preview_payload):
        """Test featurelist sql (success)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(f"{self.base_route}/sql", json=featurelist_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert response.json().endswith(
            'SELECT\n  "_fb_internal_window_w1800_sum_d96824b6af9f301d26d9bd64801d0cd10ab5fe8f" AS "sum_30m"\n'
            "FROM _FB_AGGREGATED AS AGG"
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
        groupby_node = graph.get_node_by_name("groupby_1").parameters.dict()
        groupby_node["names"] = ["sum_30m"]
        groupby_node["windows"] = ["30m"]
        assert feature_clusters[0] == expected_feature_cluster

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
        feature_list_doc = create_success_response.json()
        self._make_production_ready_and_deploy(test_api_client, feature_list_doc)

        # Request online features
        feature_list_id = feature_list_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{feature_list_id}/online_features",
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
        feature_list_doc = create_success_response.json()

        # Request online features before deploying
        feature_list_id = feature_list_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{feature_list_id}/online_features",
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
        feature_list_doc = create_success_response.json()

        # Request online features before deploying
        feature_list_id = feature_list_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}] * num_rows}
        response = test_api_client.post(
            f"{self.base_route}/{feature_list_id}/online_features",
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

        # Deploy feature list
        feature_list_doc = create_success_response.json()
        self._make_production_ready_and_deploy(test_api_client, feature_list_doc)

        # Request online features
        feature_list_id = feature_list_doc["_id"]
        data = {"entity_serving_names": [{"cust_id": 1}]}
        response = test_api_client.post(
            f"{self.base_route}/{feature_list_id}/online_features",
            data=json.dumps(data),
        )
        assert response.status_code == HTTPStatus.OK, response.content

        # Check result
        assert response.json() == {"features": [{"cust_id": 1.0, "feature_value": None}]}

    @freeze_time("2022-01-02 10:00:00")
    def test_get_feature_job_logs_200(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
    ):
        """Test get feature job logs"""
        test_api_client, _ = test_api_client_persistent
        featurelist = create_success_response.json()
        feature_list_id = featurelist["_id"]
        graph = QueryGraphModel(**featurelist["feature_clusters"][0]["graph"])
        groupby_node = graph.get_node_by_name("groupby_1")
        aggregation_id = groupby_node.parameters.aggregation_id

        job_logs = pd.DataFrame(
            {
                "SESSION_ID": ["SID1"] * 4 + ["SID2"] * 2,
                "AGGREGATION_ID": [aggregation_id] * 6,
                "CREATED_AT": pd.to_datetime(
                    [
                        "2020-01-02 18:00:00",
                        "2020-01-02 18:01:00",
                        "2020-01-02 18:02:00",
                        "2020-01-02 18:03:00",
                        "2020-01-02 18:00:00",
                        "2020-01-02 18:05:00",
                    ]
                ),
                "STATUS": [
                    "STARTED",
                    "MONITORED",
                    "GENERATED",
                    "COMPLETED",
                    "STARTED",
                    "GENERATED_FAILED",
                ],
                "MESSAGE": [""] * 5 + ["Some error has occurred"],
            }
        )
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = job_logs
        response = test_api_client.get(f"{self.base_route}/{feature_list_id}/feature_job_logs")
        assert response.status_code == HTTPStatus.OK
        expected_df = pd.DataFrame(
            {
                "SESSION_ID": ["SID1", "SID2"],
                "AGGREGATION_ID": [aggregation_id] * 2,
                "SCHEDULED": pd.to_datetime(["2020-01-02 17:35:00"] * 2),
                "STARTED": pd.to_datetime(["2020-01-02 18:00:00"] * 2),
                "COMPLETED": pd.to_datetime(["2020-01-02 18:03:00", pd.NaT]),
                "QUEUE_DURATION": [1500.0] * 2,
                "COMPUTE_DURATION": [180.0, np.nan],
                "TOTAL_DURATION": [1680.0, np.nan],
                "ERROR": [np.nan, "Some error has occurred"],
            }
        )
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)
        assert (
            mock_session.execute_query.call_args[0][0]
            == textwrap.dedent(
                f"""
            SELECT
              "SESSION_ID",
              "CREATED_AT",
              "AGGREGATION_ID",
              "STATUS",
              "MESSAGE"
            FROM TILE_JOB_MONITOR
            WHERE
              "CREATED_AT" >= CAST('2022-01-01 10:00:00' AS TIMESTAMPNTZ)
              AND "CREATED_AT" < CAST('2022-01-02 10:00:00' AS TIMESTAMPNTZ)
              AND "AGGREGATION_ID" IN ('{aggregation_id}')
              AND "TILE_TYPE" = 'ONLINE'
            """
            ).strip()
        )
