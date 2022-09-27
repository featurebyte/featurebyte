"""
Tests for Feature route
"""
from collections import defaultdict
from datetime import datetime
from http import HTTPStatus
from unittest.mock import patch

import pytest
from bson.objectid import ObjectId

from featurebyte.common.model_util import get_version
from featurebyte.exception import CredentialsError
from tests.unit.routes.base import BaseApiTestSuite


class TestFeatureApi(BaseApiTestSuite):
    """
    TestFeatureApi class
    """

    class_name = "Feature"
    base_route = "/feature"
    payload = BaseApiTestSuite.load_payload("tests/fixtures/request_payloads/feature_sum_30m.json")
    namespace_payload = BaseApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_namespace.json"
    )
    object_id = str(ObjectId())
    create_conflict_payload_expected_detail_pairs = [
        (
            payload,
            f'Feature (id: "{payload["_id"]}") already exists. '
            f'Get the existing object by `Feature.get_by_id(id="{payload["_id"]}")`.',
        ),
        (
            {
                **payload,
                "_id": str(ObjectId()),
                "feature_namespace_id": object_id,
            },
            'FeatureNamespace (name: "sum_30m") already exists. '
            'Please rename object (name: "sum_30m") to something else.',
        ),
    ]
    create_unprocessable_payload_expected_detail_pairs = [
        (
            {**payload, "event_data_ids": []},
            [
                {
                    "loc": ["body", "event_data_ids"],
                    "msg": "ensure this value has at least 1 items",
                    "type": "value_error.list.min_items",
                    "ctx": {"limit_value": 1},
                }
            ],
        ),
        (
            {**payload, "_id": object_id, "name": "random_name", "event_data_ids": [object_id]},
            f'EventData (id: "{object_id}") not found. ' f"Please save the EventData object first.",
        ),
        (
            {**payload, "_id": object_id, "name": "random_name"},
            (
                'Feature (name: "random_name") object(s) within the same namespace must have '
                'the same "name" value (namespace: "sum_30m", feature: "random_name").'
            ),
        ),
        (
            {
                **payload,
                "_id": object_id,
                "entity_ids": ["631161373527e8d21e4197ac"],
            },
            (
                'Feature (name: "sum_30m") object(s) within the same namespace must have '
                "the same \"entity_ids\" value (namespace: ['6332dfc6d9532cdb02748f87'], "
                "feature: ['631161373527e8d21e4197ac'])."
            ),
        ),
    ]

    @pytest.fixture(autouse=True)
    def mock_insert_feature_registry_fixture(self):
        """
        Mock insert feature registry at the controller level
        """
        with patch("featurebyte.service.feature.FeatureService._insert_feature_registry") as mock:
            yield mock

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["feature_namespace_id"] = str(ObjectId())
            tabular_source = payload["tabular_source"]
            payload["tabular_source"] = {
                "feature_store_id": tabular_source["feature_store_id"],
                "table_details": {
                    key: f"{value}_{i}" for key, value in tabular_source["table_details"].items()
                },
            }
            yield payload

    @pytest.mark.asyncio
    async def test_create_201(
        self, test_api_client_persistent, create_success_response, user_id
    ):  # pylint: disable=invalid-overridden-method
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        response_dict = create_success_response.json()
        assert response_dict["readiness"] == "DRAFT"
        assert response_dict["version"] == {"name": get_version(), "suffix": None}

        # check feature namespace
        test_api_client, persistent = test_api_client_persistent
        feat_namespace_docs, match_count = await persistent.find(
            collection_name="feature_namespace",
            query_filter={"name": self.payload["name"]},
        )
        assert match_count == 1
        assert feat_namespace_docs[0]["name"] == self.payload["name"]
        assert feat_namespace_docs[0]["feature_ids"] == [ObjectId(self.payload["_id"])]
        assert feat_namespace_docs[0]["readiness"] == "DRAFT"
        assert feat_namespace_docs[0]["default_feature_id"] == ObjectId(self.payload["_id"])
        assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
        assert feat_namespace_docs[0]["created_at"] >= datetime.fromisoformat(
            response_dict["created_at"]
        )
        assert feat_namespace_docs[0]["updated_at"] is not None

        # create a new feature version with the same namespace
        new_payload = self.payload.copy()
        new_payload["_id"] = str(ObjectId())
        new_response = test_api_client.post("/feature", json=new_payload)
        new_response_dict = new_response.json()
        assert new_response.status_code == HTTPStatus.CREATED
        assert new_response_dict.items() >= new_payload.items()
        assert new_response_dict["version"] == {"name": get_version(), "suffix": 1}

        # check feature namespace with the new feature version
        feat_namespace_docs, match_count = await persistent.find(
            collection_name="feature_namespace",
            query_filter={"name": self.payload["name"]},
        )
        assert match_count == 1
        assert feat_namespace_docs[0]["name"] == self.payload["name"]
        assert feat_namespace_docs[0]["feature_ids"] == [
            ObjectId(self.payload["_id"]),
            ObjectId(new_payload["_id"]),
        ]
        assert feat_namespace_docs[0]["readiness"] == "DRAFT"
        assert feat_namespace_docs[0]["default_feature_id"] == ObjectId(new_payload["_id"])
        assert feat_namespace_docs[0]["default_version_mode"] == "AUTO"
        assert feat_namespace_docs[0]["created_at"] >= datetime.fromisoformat(
            response_dict["created_at"]
        )
        assert feat_namespace_docs[0]["updated_at"] > feat_namespace_docs[0]["created_at"]

    def test_create_401(self, test_api_client_persistent, mock_insert_feature_registry_fixture):
        """Test create (unauthorized)"""
        mock_insert_feature_registry_fixture.side_effect = CredentialsError
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        response = test_api_client.post(f"{self.base_route}", json=self.payload)
        assert response.status_code == HTTPStatus.UNAUTHORIZED

    def test_list_404__feature_list_not_found(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
    ):
        """Test list (not found) when the feature list id is not found"""
        test_api_client, _ = test_api_client_persistent
        _ = create_multiple_success_responses
        random_id = ObjectId()
        response = test_api_client.get(self.base_route, params={"feature_list_id": str(random_id)})
        error_message = (
            f'FeatureList (id: "{random_id}") not found. Please save the FeatureList object first.'
        )
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json()["detail"] == error_message

    def test_list_200__filter_by_feature_list_id(self, test_api_client_persistent):
        """Test list (success) using feature_list_id to filter"""
        test_api_client, _ = test_api_client_persistent

        # create feature list first
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
            ("event_data", "event_data"),
            ("feature", "feature_sum_30m"),
            ("feature", "feature_sum_2h"),
            ("feature_list", "feature_list_multi"),
        ]
        feature_ids = []
        feature_list_id = None
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = test_api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

            if api_object == "feature":
                feature_ids.append(response.json()["_id"])
            if api_object == "feature_list_id":
                feature_list_id = payload["_id"]

        response = test_api_client.get(self.base_route, params={"feature_list_id": feature_list_id})
        response_dict = response.json()
        output_feature_ids = [feat["_id"] for feat in response_dict["data"]]
        assert response.status_code == HTTPStatus.OK
        assert response_dict["total"] == len(feature_ids)
        assert set(output_feature_ids) == set(feature_ids)

    def test_list_200__filter_by_namespace_id(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list (filtered by feature namespace id)"""
        test_api_client, _ = test_api_client_persistent
        namespace_map = defaultdict(set)
        for success_response in create_multiple_success_responses:
            response_dict = success_response.json()
            namespace_map[response_dict["feature_namespace_id"]].add(response_dict["_id"])

        for namespace_id, ids in namespace_map.items():
            filter_response = test_api_client.get(
                self.base_route, params={"feature_namespace_id": namespace_id}
            )
            filter_response_dict = filter_response.json()
            assert filter_response_dict["total"] == len(ids)
            response_ids = set(item["_id"] for item in filter_response_dict["data"])
            assert response_ids == ids

        # test negative cases
        negative_response = test_api_client.get(
            self.base_route, params={"feature_namespace_id": str(ObjectId())}
        )
        assert negative_response.json()["total"] == 0, negative_response.json()

    def test_update_200(self, test_api_client_persistent, create_success_response):
        """Test update (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        assert create_response_dict["readiness"] == "DRAFT"
        doc_id = create_response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["readiness"] == "PRODUCTION_READY"

        # test online enabled
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"online_enabled": True}
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["online_enabled"] is True

        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"online_enabled": False}
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["online_enabled"] is False

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
            "name": "sum_30m",
            "entities": [{"name": "customer", "serving_names": ["cust_id"]}],
            "event_data": [{"name": "sf_event_data", "status": "DRAFT"}],
            "dtype": "FLOAT",
            "default_version_mode": "AUTO",
            "version_count": 1,
            "readiness": {"this": "DRAFT", "default": "DRAFT"},
            "version": {"this": version, "default": version},
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
