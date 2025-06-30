"""
Tests for Feature route
"""

import collections
import textwrap
from collections import defaultdict
from datetime import datetime
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
from tests.unit.common.test_utils import create_batch_feature_create
from tests.unit.routes.base import BaseCatalogApiTestSuite
from tests.util.helper import compare_pydantic_obj


class TestFeatureApi(BaseCatalogApiTestSuite):
    """
    TestFeatureApi class
    """

    class_name = "Feature"
    base_route = "/feature"
    payload = BaseCatalogApiTestSuite.load_payload(
        "tests/fixtures/request_payloads/feature_sum_30m.json"
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
                    "input": {"name": "value"},
                    "loc": ["body", "FeatureCreate", "graph", "edges"],
                    "msg": "Input should be a valid list",
                    "type": "list_type",
                },
                {
                    "input": {
                        "_COMMENT": payload["_COMMENT"],
                        "_id": payload["_id"],
                        "graph": {"edges": {"name": "value"}},
                        "name": "sum_30m",
                        "node_name": "project_1",
                        "tabular_source": {
                            "feature_store_id": payload["tabular_source"]["feature_store_id"],
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_table",
                            },
                        },
                    },
                    "loc": ["body", "FeatureNewVersionCreate", "source_feature_id"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ],
        ),
        (
            {**payload, "graph": {"nodes": {}}},
            [
                {
                    "input": {},
                    "loc": ["body", "FeatureCreate", "graph", "nodes"],
                    "msg": "Input should be a valid list",
                    "type": "list_type",
                },
                {
                    "input": {
                        "_COMMENT": payload["_COMMENT"],
                        "_id": payload["_id"],
                        "graph": {"nodes": {}},
                        "name": "sum_30m",
                        "node_name": "project_1",
                        "tabular_source": {
                            "feature_store_id": payload["tabular_source"]["feature_store_id"],
                            "table_details": {
                                "database_name": "sf_database",
                                "schema_name": "sf_schema",
                                "table_name": "sf_table",
                            },
                        },
                    },
                    "loc": ["body", "FeatureNewVersionCreate", "source_feature_id"],
                    "msg": "Field required",
                    "type": "missing",
                },
            ],
        ),
        (
            {**payload, "node_name": "groupby_1"},
            (
                "1 validation error for FeatureModel\n"
                "  Value error, Feature or target graph must have exactly one aggregation output [type=value_error, "
                "input_value={'_id': ObjectId('646f6c1...de_serving_names': True}, input_type=dict]\n"
                "    For further information visit https://errors.pydantic.dev/2.8/v/value_error"
            ),
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
        self, patched_observation_table_service_for_preview
    ):
        """
        Patch ObservationTableService so validate_materialized_table_and_get_metadata always passes
        """
        _ = patched_observation_table_service_for_preview

    def setup_creation_route(self, api_client):
        """Setup for post route"""
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
            ("context", "context"),
            ("event_table", "event_table"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(f"/{api_object}", json=payload)
            assert response.status_code == HTTPStatus.CREATED

            if api_object.endswith("_table"):
                # tag table entity for table objects
                self.tag_table_entity(api_client, api_object, payload)

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        for i in range(3):
            payload = self.payload.copy()
            payload["_id"] = str(ObjectId())
            payload["name"] = f'{self.payload["name"]}_{i}'
            tabular_source = payload["tabular_source"]
            payload["tabular_source"] = {
                "feature_store_id": tabular_source["feature_store_id"],
                "table_details": {
                    key: f"{value}_{i}" for key, value in tabular_source["table_details"].items()
                },
            }
            yield payload

    @pytest.mark.asyncio
    async def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        response_dict = create_success_response.json()
        assert response_dict["readiness"] == "DRAFT"
        assert response_dict["version"] == {"name": get_version(), "suffix": None}

        # check feature namespace
        _, persistent = test_api_client_persistent
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
        assert feat_namespace_docs[0]["feature_type"] == "numeric"

    def test_create_201__create_new_version(
        self, test_api_client_persistent, create_success_response
    ):
        """Test new version creation (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        graph_origin = QueryGraphModel(**create_response_dict["graph"])
        graph_node_origin = graph_origin.get_node_by_name("graph_1")
        assert graph_node_origin.parameters.metadata.column_cleaning_operations == []

        # create a new version
        column_cleaning_operations = [
            {
                "column_name": "col_float",
                "cleaning_operations": [{"type": "missing", "imputed_value": 0.0}],
            },
        ]
        response = test_api_client.post(
            f"{self.base_route}",
            json={
                "source_feature_id": create_response_dict["_id"],
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
                        "feature_job_setting": {
                            "blind_spot": "1d",
                            "period": "1d",
                            "offset": "1h",
                        },
                    }
                ],
                "table_cleaning_operations": [
                    {
                        "table_name": "sf_event_table",
                        "column_cleaning_operations": column_cleaning_operations,
                    }
                ],
            },
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED
        assert response_dict["version"] == {"name": get_version(), "suffix": 1}
        assert response_dict["relationships_info"] == []

        # check feature job setting using the specified feature job setting
        graph = QueryGraphModel(**response_dict["graph"])
        groupby_node = graph.get_node_by_name("groupby_1")
        parameters = groupby_node.parameters.model_dump()
        assert parameters["feature_job_setting"] == {
            "offset": "3600s",
            "period": "86400s",
            "blind_spot": "86400s",
            "execution_buffer": "0s",
        }

        # check that the table cleaning operations are applied
        graph_node = graph.get_node_by_name("graph_1")
        compare_pydantic_obj(
            graph_node.parameters.metadata.column_cleaning_operations,
            column_cleaning_operations,
        )

    def test_create_422__create_new_version(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create new version (unprocessable entity)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        response = test_api_client.post(
            self.base_route,
            json={"source_feature_id": create_response_dict["_id"]},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        response_dict = response.json()
        assert response_dict["detail"] == "No change detected on the new feature version."

    def test_create_422__create_new_version__unrelated_cleaning_operations(
        self, test_api_client_persistent, create_success_response
    ):
        """Test create new version (unprocessable entity due to unrelated cleaning operations)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        response = test_api_client.post(
            f"{self.base_route}",
            json={
                "source_feature_id": create_response_dict["_id"],
                "table_cleaning_operations": [
                    {"table_name": "random_data", "column_cleaning_operations": []}
                ],
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        response_dict = response.json()
        expected_msg = (
            "Table cleaning operation(s) does not result a new feature version. "
            "This is because the new feature version is the same as the source feature."
        )
        assert response_dict["detail"] == expected_msg

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
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
                        "feature_job_setting": {
                            "blind_spot": "1d",
                            "period": "1d",
                            "offset": "1h",
                        },
                    }
                ],
            },
        )

        # check retrieving old feature version
        version = create_response_dict["version"]["name"]
        response = test_api_client.get(
            self.base_route, params={"name": create_response_dict["name"], "version": version}
        )
        response_dict = response.json()
        create_response_dict["is_default"] = False
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
            ("entity", "entity"),
            ("event_table", "event_table"),
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
            if api_object == "feature_list":
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

    def test_update_200_and_422(self, test_api_client_persistent, create_success_response):
        """Test update (success & unprocessable entity)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        assert create_response_dict["readiness"] == "DRAFT"
        doc_id = create_response_dict["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["readiness"] == "PRODUCTION_READY"

        # create a new version & attempt to update to production ready
        new_feature_id = self.create_new_feature_version(test_api_client, doc_id)
        response = test_api_client.patch(
            f"{self.base_route}/{new_feature_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        expected_error_message = (
            "Found another feature version that is already PRODUCTION_READY. "
            f'Please deprecate the feature "sum_30m" with ID {doc_id} first '
            f"before promoting the promoted version as there can only be one feature version "
            f"that is production ready at any point in time. "
            f"We are unable to promote the feature with ID {new_feature_id} right now."
        )
        assert response.json()["detail"] == expected_error_message

        # deprecate the original feature
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"readiness": "DEPRECATED"}
        )
        assert response.status_code == HTTPStatus.OK

        # promote the new feature to production ready
        response = test_api_client.patch(
            f"{self.base_route}/{new_feature_id}", json={"readiness": "PRODUCTION_READY"}
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        expected_error_message = (
            "Discrepancies found between the promoted feature version you are trying to promote to "
            "PRODUCTION_READY, and the input table.\n"
            "{'feature_job_setting': {"
            "'data_source': FeatureJobSetting(blind_spot='600s', period='1800s', offset='300s', execution_buffer='0s'), "
            "'promoted_feature': "
            "FeatureJobSetting(blind_spot='82800s', period='86400s', offset='3600s', execution_buffer='0s')}}\n"
            "Please fix these issues first before trying to promote your feature to PRODUCTION_READY."
        )
        assert response.json()["detail"] == expected_error_message

    def test_delete_204(self, test_api_client_persistent, create_success_response):
        """Test delete (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        namespace_id = create_response_dict["feature_namespace_id"]

        namespace_response = test_api_client.get(f"/feature_namespace/{namespace_id}")
        assert namespace_response.status_code == HTTPStatus.OK
        assert namespace_response.json()["feature_ids"] == [doc_id]

        # delete feature
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK

        # check that the feature & feature namespace are deleted
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

        response = test_api_client.get(f"/feature_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

    def test_delete_204__namespace_not_deleted(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        namespace_id = create_response_dict["feature_namespace_id"]

        # create another feature in the same namespace
        new_feature_id = self.create_new_feature_version(test_api_client, doc_id)

        # check namespace before delete
        namespace_dict = test_api_client.get(f"/feature_namespace/{namespace_id}").json()
        assert namespace_dict["feature_ids"] == [doc_id, new_feature_id]

        # delete feature
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK

        # check namespace after delete
        namespace_dict = test_api_client.get(f"/feature_namespace/{namespace_id}").json()
        assert namespace_dict["feature_ids"] == [new_feature_id]

    def check_that_feature_is_not_deleted(self, test_api_client, doc_id, namespace_id):
        """Check that the feature & feature namespace are not deleted"""
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.OK

        response = test_api_client.get(f"/feature_namespace/{namespace_id}")
        assert response.status_code == HTTPStatus.OK

    def test_delete_422__non_draft_feature(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete (unprocessible)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # change feature readiness to public draft
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}", json={"readiness": "PUBLIC_DRAFT"}
        )
        assert response.status_code == HTTPStatus.OK

        # check that the feature cannot be deleted
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        expected_error = "Only feature with draft readiness can be deleted."
        assert response.json()["detail"] == expected_error

        # check that the feature is not deleted
        self.check_that_feature_is_not_deleted(
            test_api_client, doc_id, create_response_dict["feature_namespace_id"]
        )

    def test_delete_422__manual_mode_default_feature(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete (unprocessible)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]
        namespace_id = create_response_dict["feature_namespace_id"]

        # set the feature namespace default version mode to manual first
        response = test_api_client.patch(
            f"/feature_namespace/{namespace_id}", json={"default_version_mode": "MANUAL"}
        )
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["default_version_mode"] == "MANUAL"
        assert response_dict["default_feature_id"] == doc_id

        # check that the feature cannot be deleted
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        expected_error = (
            "Feature is the default feature of the feature namespace and the default version mode is manual. "
            "Please set another feature as the default feature or change the default version mode to auto."
        )
        assert response.json()["detail"] == expected_error

        # check that the feature is not deleted
        self.check_that_feature_is_not_deleted(test_api_client, doc_id, namespace_id)

    def test_delete_422__feature_used_in_a_saved_feature_list(
        self, test_api_client_persistent, create_success_response
    ):
        """Test delete (unprocessible)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        # use the feature to create a saved feature list
        response = test_api_client.post(
            "/feature_list", json={"name": "test", "feature_ids": [doc_id]}
        )
        feature_list_dict = response.json()
        feature_list_id = feature_list_dict["_id"]
        feature_list_version = feature_list_dict["version"]["name"]
        assert response.status_code == HTTPStatus.CREATED
        assert feature_list_dict["feature_ids"] == [doc_id]

        # check that the feature cannot be deleted
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

        expected_error = (
            "Feature is still in use by feature list(s). Please remove the following feature list(s) first:\n"
            f"[{{'id': '{feature_list_id}', 'name': 'test', 'version': '{feature_list_version}'}}]"
        )
        assert response.json()["detail"] == expected_error

        # check that the feature is not deleted
        self.check_that_feature_is_not_deleted(
            test_api_client, doc_id, create_response_dict["feature_namespace_id"]
        )

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
                {"name": "customer", "serving_names": ["cust_id"], "catalog_name": "grocery"}
            ],
            "tables": [
                {"name": "sf_event_table", "status": "PUBLIC_DRAFT", "catalog_name": "grocery"}
            ],
            "dtype": "FLOAT",
            "default_version_mode": "AUTO",
            "version_count": 1,
            "readiness": {"this": "DRAFT", "default": "DRAFT"},
            "version": {"this": version, "default": version},
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

    @pytest.fixture(name="feature_preview_payload")
    def feature_preview_payload_fixture(self, create_success_response):
        """
        feature_preview_payload fixture
        """
        feature = create_success_response.json()
        return {
            "graph": feature["graph"],
            "node_name": feature["node_name"],
            "point_in_time_and_serving_name_list": [
                {
                    "cust_id": "C1",
                    "POINT_IN_TIME": "2022-04-01",
                },
                {
                    "cust_id": "C3",
                    "POINT_IN_TIME": "2022-04-03",
                },
            ],
            "feature_store_id": self.payload["tabular_source"]["feature_store_id"],
        }

    def test_preview_200(
        self,
        test_api_client_persistent,
        feature_preview_payload,
        mock_get_session,
    ):
        """Test feature preview (success)"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using graph and node name
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_using_feature_id_200(
        self,
        test_api_client_persistent,
        create_success_response,
        feature_preview_payload,
        mock_get_session,
    ):
        """Test feature preview (success)"""
        test_api_client, _ = test_api_client_persistent
        feature = create_success_response.json()
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = expected_df
        mock_session.generate_session_unique_id = Mock(return_value="1")

        # test preview using feature id
        feature_preview_payload.pop("graph")
        feature_preview_payload.pop("node_name")
        feature_preview_payload["feature_id"] = feature["_id"]
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.OK
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_using_observation_table_200(
        self,
        test_api_client_persistent,
        feature_preview_payload,
        mock_get_session,
    ):
        """Test feature preview (success)"""
        test_api_client, _ = test_api_client_persistent
        expected_df = pd.DataFrame({"a": [0, 1, 2]})
        mock_session = mock_get_session.return_value
        mock_session.list_table_schema.return_value = collections.OrderedDict({
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

        feature_preview_payload.pop("point_in_time_and_serving_name_list")
        feature_preview_payload["observation_table_id"] = "646f6c1c0ed28a5271fb02d7"
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.OK, response.json()
        assert_frame_equal(dataframe_from_json(response.json()), expected_df)

    def test_preview_missing_point_in_time(
        self, test_api_client_persistent, feature_preview_payload
    ):
        """
        Test feature preview validation missing point in time
        """
        test_api_client, _ = test_api_client_persistent
        feature_preview_payload["point_in_time_and_serving_name_list"][0] = {
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
        feature_preview_payload["point_in_time_and_serving_name_list"][0] = {
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
        feature_preview_payload["point_in_time_and_serving_name_list"][0] = tuple([
            "2022-04-01",
            "C1",
        ])
        response = test_api_client.post(f"{self.base_route}/preview", json=feature_preview_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "input": ["2022-04-01", "C1"],
                "loc": ["body", "point_in_time_and_serving_name_list", 0],
                "msg": "Input should be a valid dictionary",
                "type": "dict_type",
            }
        ]

    def test_sql_200(self, test_api_client_persistent, feature_preview_payload):
        """Test feature sql (success)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(f"{self.base_route}/sql", json=feature_preview_payload)
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

    @freeze_time("2022-01-02 10:00:00")
    def test_get_feature_job_logs_200(
        self,
        test_api_client_persistent,
        create_success_response,
    ):
        """Test get feature job logs"""
        test_api_client, _ = test_api_client_persistent
        feature_doc = create_success_response.json()
        feature_id = feature_doc["_id"]
        graph = QueryGraphModel(**feature_doc["graph"])
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
            response = test_api_client.get(f"{self.base_route}/{feature_id}/feature_job_logs")
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
    async def test_batch_feature_create__success(
        self, test_api_client_persistent, mock_snowflake_session, user_id, app_container
    ):
        """Test batch feature create async task"""
        _ = mock_snowflake_session
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # prepare batch feature create payload
        payload_1 = self.payload.copy()
        payload_2 = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature_create_1 = FeatureCreate(**payload_1)
        feature_create_2 = FeatureCreate(**payload_2)
        feature_creates = [feature_create_1, feature_create_2]
        batch_feature_create = create_batch_feature_create(features=feature_creates)

        # check feature is not created
        for feat_create in feature_creates:
            response = test_api_client.get(f"{self.base_route}/{feat_create.id}")
            assert response.status_code == HTTPStatus.NOT_FOUND

        # create batch feature create task
        task_response = test_api_client.post(
            f"{self.base_route}/batch", json=batch_feature_create.json_dict()
        )

        # check user id
        task_dict = task_response.json()
        assert task_dict["payload"]["user_id"] == str(user_id)

        # retrieve task results
        response = self.wait_for_results(test_api_client, task_response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS"
        assert response_dict["output_path"] is None
        assert response_dict["traceback"] is None

        # check feature is created
        for feat_create in feature_creates:
            response = test_api_client.get(f"{self.base_route}/{feat_create.id}")
            response_dict = response.json()
            assert response_dict["name"] == feat_create.name
            assert response.status_code == HTTPStatus.OK

        # check task result
        task_manager = app_container.task_manager
        task_result = await task_manager.get_task_result(task_dict["id"])
        assert set(task_result) == {feature_create_1.id, feature_create_2.id}

    @pytest.mark.asyncio
    @patch(
        "featurebyte.worker.util.batch_feature_creator.BatchFeatureCreator.is_generated_feature_consistent",
        new_callable=AsyncMock,
    )
    async def test_batch_feature_create__failure(
        self,
        mock_is_generated_feature_consistent,
        test_api_client_persistent,
        mock_snowflake_session,
    ):
        """Test batch feature create async task"""
        _ = mock_snowflake_session
        mock_is_generated_feature_consistent.side_effect = [False, True]
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)

        # prepare batch feature create payload
        another_payload = self.load_payload("tests/fixtures/request_payloads/feature_sum_2h.json")
        feature_create1 = FeatureCreate(**self.payload)
        feature_create2 = FeatureCreate(**another_payload)
        batch_feature_create = create_batch_feature_create(
            features=[feature_create1, feature_create2]
        )

        # create batch feature create task
        task_response = test_api_client.post(
            f"{self.base_route}/batch", json=batch_feature_create.json_dict()
        )
        response = self.wait_for_results(test_api_client, task_response)
        response_dict = response.json()
        expected_traceback = (
            "Inconsistent feature definitions detected: sum_30m\n"
            "The inconsistent features have been deleted."
        )
        assert expected_traceback in response_dict["traceback"]
        assert response_dict["status"] == "FAILURE"

        # check feature1 is not created
        response = test_api_client.get(f"{self.base_route}/{feature_create1.id}")
        assert response.status_code == HTTPStatus.NOT_FOUND

        # check feature2 is created
        response = test_api_client.get(f"{self.base_route}/{feature_create2.id}")
        assert response.status_code == HTTPStatus.OK

    def test_request_sample_entity_serving_names(
        self,
        test_api_client_persistent,
        create_success_response,
        mock_get_session,
    ):
        """Test getting sample entity serving names for a feature"""
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
        feature_id = result["_id"]
        response = test_api_client.get(
            f"{self.base_route}/{feature_id}/sample_entity_serving_names?count=10",
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

    def test_delete_event_table(self, test_api_client_persistent, create_success_response):
        """Test delete event table"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        table_id = create_response_dict["table_ids"][0]

        # untag id column's entity from the table first
        response = test_api_client.patch(
            f"event_table/{table_id}/column_entity",
            json={"column_name": "col_int", "entity_id": None},
        )
        assert response.status_code == HTTPStatus.OK, response.json()

        # attempt to delete an event table used by a feature
        response = test_api_client.delete(f"/event_table/{table_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "EventTable is referenced by Feature: sum_30m"

        # delete the feature & then delete the event table
        feature_id = create_response_dict["_id"]
        response = test_api_client.delete(f"{self.base_route}/{feature_id}")
        assert response.status_code == HTTPStatus.OK, response.json()
        response = test_api_client.delete(f"/event_table/{table_id}")
        assert response.status_code == HTTPStatus.OK, response.json()

        # check deleted table
        response = test_api_client.get(f"/event_table/{table_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_entity(self, test_api_client_persistent, create_success_response):
        """Test delete entity used by the feature"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        entity_id = create_response_dict["entity_ids"][0]
        response = test_api_client.delete(f"/entity/{entity_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == "Entity is referenced by Feature: sum_30m"
