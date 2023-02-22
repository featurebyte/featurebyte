"""
Tests for Feature route
"""
import textwrap
from collections import defaultdict
from datetime import datetime
from http import HTTPStatus
from unittest.mock import Mock

import numpy as np
import pandas as pd
import pytest
from bson.objectid import ObjectId
from freezegun import freeze_time
from pandas.testing import assert_frame_equal

from featurebyte.common.model_util import get_version
from featurebyte.common.utils import dataframe_from_json
from featurebyte.models.base import DEFAULT_WORKSPACE_ID
from tests.unit.routes.base import BaseWorkspaceApiTestSuite


class TestFeatureApi(BaseWorkspaceApiTestSuite):
    """
    TestFeatureApi class
    """

    class_name = "Feature"
    base_route = "/feature"
    payload = BaseWorkspaceApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_sum_30m.json"
    )
    namespace_payload = BaseWorkspaceApiTestSuite.load_payload(
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
            {**payload, "graph": {"edges": {"name": "value"}}},
            [
                {
                    "loc": ["body", "graph", "edges"],
                    "msg": "value is not a valid list",
                    "type": "type_error.list",
                },
                {
                    "loc": ["body", "source_feature_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        ),
        (
            {**payload, "graph": {"nodes": {}}},
            [
                {
                    "loc": ["body", "graph", "nodes"],
                    "msg": "value is not a valid list",
                    "type": "type_error.list",
                },
                {
                    "loc": ["body", "source_feature_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        ),
        (
            {**payload, "tabular_data_ids": []},
            [
                {
                    "loc": ["body", "tabular_data_ids"],
                    "msg": "ensure this value has at least 1 items",
                    "type": "value_error.list.min_items",
                    "ctx": {"limit_value": 1},
                },
                {
                    "loc": ["body", "source_feature_id"],
                    "msg": "field required",
                    "type": "value_error.missing",
                },
            ],
        ),
        (
            {**payload, "_id": object_id, "name": "random_name", "tabular_data_ids": [object_id]},
            f'TabularData (id: "{object_id}") not found. '
            f"Please save the TabularData object first.",
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
                "the same \"entity_ids\" value (namespace: ['63a443938bcb22a734625955'], "
                "feature: ['631161373527e8d21e4197ac'])."
            ),
        ),
    ]

    def setup_creation_route(self, api_client, workspace_id=DEFAULT_WORKSPACE_ID):
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
            response = api_client.post(
                f"/{api_object}", params={"workspace_id": workspace_id}, json=payload
            )
            assert response.status_code == HTTPStatus.CREATED

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
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
        assert feat_namespace_docs[0]["updated_at"] is None

        # create a new feature version with the same namespace
        new_payload = self.payload.copy()
        new_payload["_id"] = str(ObjectId())
        new_response = test_api_client.post("/feature", json=new_payload)
        new_response_dict = new_response.json()
        # graph gets aggressively pruned during saving and hash is regenerated
        expected_response = new_payload.copy()
        expected_response["graph"] = new_response_dict["graph"]
        assert new_response.status_code == HTTPStatus.CREATED
        assert new_response_dict.items() >= expected_response.items()
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

    def test_create_201__create_new_version(
        self, test_api_client_persistent, create_success_response
    ):
        """Test new version creation (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        response = test_api_client.post(
            f"{self.base_route}",
            json={
                "source_feature_id": create_response_dict["_id"],
                "feature_job_setting": {
                    "blind_spot": "1d",
                    "frequency": "1d",
                    "time_modulo_frequency": "1h",
                },
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED
        assert response_dict["version"] == {"name": get_version(), "suffix": 1}

        groupby_node = response_dict["graph"]["nodes"][1]
        assert groupby_node["name"] == "groupby_1"

        parameters = groupby_node["parameters"]
        assert parameters["time_modulo_frequency"] == 3600
        assert parameters["frequency"] == 86400
        assert parameters["blind_spot"] == 86400

    def test_create_422__create_new_version(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create new version (unprocessable entity)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        response = test_api_client.post(
            f"{self.base_route}",
            json={"source_feature_id": create_response_dict["_id"]},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        response_dict = response.json()
        assert response_dict["detail"] == "No change detected on the new feature version."

    def test_list_200__filter_by_name_and_version(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """Test list (success) when filtering by name and version"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_multiple_success_responses[0].json()
        new_version_response = test_api_client.post(
            f"{self.base_route}",
            json={
                "source_feature_id": create_response_dict["_id"],
                "feature_job_setting": {
                    "blind_spot": "1d",
                    "frequency": "1d",
                    "time_modulo_frequency": "1h",
                },
            },
        )

        # check retrieving old feature version
        version = create_response_dict["version"]["name"]
        response = test_api_client.get(
            self.base_route, params={"name": create_response_dict["name"], "version": version}
        )
        response_dict = response.json()
        assert response_dict["total"] == 1
        assert response_dict["data"] == [create_response_dict]

        # check retrieving new feature version
        response = test_api_client.get(
            self.base_route,
            params={"name": create_response_dict["name"], "version": f"{version}_1"},
        )
        response_dict = response.json()
        assert response_dict["total"] == 1
        assert response_dict["data"] == [new_version_response.json()]

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
            "entities": [
                {"name": "customer", "serving_names": ["cust_id"], "workspace_name": "default"}
            ],
            "tabular_data": [
                {"name": "sf_event_data", "status": "DRAFT", "workspace_name": "default"}
            ],
            "dtype": "FLOAT",
            "default_version_mode": "AUTO",
            "version_count": 1,
            "readiness": {"this": "DRAFT", "default": "DRAFT"},
            "version": {"this": version, "default": version},
            "workspace_name": "default",
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

    @pytest.fixture(name="feature_preview_payload")
    def feature_preview_payload_fixture(self, create_success_response, test_api_client_persistent):
        """
        feature_preview_payload fixture
        """
        test_api_client, _ = test_api_client_persistent
        feature = create_success_response.json()

        feature_store_id = feature["tabular_source"]["feature_store_id"]
        response = test_api_client.get(f"/feature_store/{feature_store_id}")
        assert response.status_code == HTTPStatus.OK
        feature_store = response.json()

        return {
            "feature_store_name": feature_store["name"],
            "graph": feature["graph"],
            "node_name": feature["node_name"],
            "point_in_time_and_serving_name": {
                "cust_id": "C1",
                "POINT_IN_TIME": "2022-04-01",
            },
        }

    def test_preview_200(
        self, test_api_client_persistent, feature_preview_payload, mock_get_session
    ):
        """Test feature preview (success)"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_missing_point_in_time(
        self, test_api_client_persistent, feature_preview_payload
    ):
        """
        Test feature preview validation missing point in time
        """
        test_api_client, _ = test_api_client_persistent
        feature_preview_payload["point_in_time_and_serving_name"] = {
            "cust_id": "C1",
        }
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == "Point in time column not provided: POINT_IN_TIME"

    def test_preview_missing_entity_id(self, test_api_client_persistent, feature_preview_payload):
        """
        Test feature preview validation missing point in time
        """
        test_api_client, _ = test_api_client_persistent
        feature_preview_payload["point_in_time_and_serving_name"] = {
            "POINT_IN_TIME": "2022-04-01",
        }
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert "Required entities are not provided in the request" in response.json()["detail"]

    def test_preview_not_a_dict(self, test_api_client_persistent, feature_preview_payload):
        """
        Test feature preview validation but dict is not provided
        """
        test_api_client, _ = test_api_client_persistent
        feature_preview_payload["point_in_time_and_serving_name"] = tuple(["2022-04-01", "C1"])
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "loc": ["body", "point_in_time_and_serving_name"],
                "msg": "value is not a valid dict",
                "type": "type_error.dict",
            }
        ]

    def test_sql_200(self, test_api_client_persistent, feature_preview_payload):
        """Test feature sql (success)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(f"{self.base_route}/sql", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert response.json().endswith(
            'SELECT\n  "agg_w1800_sum_fba233e0f502088c233315a322f4c51e939072c0" AS "sum_30m"\n'
            "FROM _FB_AGGREGATED AS AGG"
        )

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
        feature_id = featurelist["_id"]

        job_logs = pd.DataFrame(
            {
                "SESSION_ID": ["SID1"] * 4 + ["SID2"] * 2,
                "AGGREGATION_ID": ["sum_fba233e0f502088c233315a322f4c51e939072c0"] * 6,
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
        response = test_api_client.get(f"{self.base_route}/{feature_id}/feature_job_logs")
        assert response.status_code == HTTPStatus.OK
        expected_df = pd.DataFrame(
            {
                "SESSION_ID": ["SID1", "SID2"],
                "AGGREGATION_ID": ["sum_fba233e0f502088c233315a322f4c51e939072c0"] * 2,
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
                """
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
              AND "AGGREGATION_ID" IN ('sum_fba233e0f502088c233315a322f4c51e939072c0')
              AND "TILE_TYPE" = 'ONLINE'
            """
            ).strip()
        )
