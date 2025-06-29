"""
BaseApiTestSuite
"""

import json
import os
import tempfile
import textwrap
from datetime import datetime
from http import HTTPStatus
from io import BytesIO
from pathlib import Path
from time import sleep
from unittest.mock import Mock, patch

import pandas as pd
import pyarrow as pa
import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.api.utils import parquet_from_arrow_stream
from featurebyte.common.utils import (
    ResponseStream,
    create_new_arrow_stream_writer,
    dataframe_to_arrow_bytes,
)
from featurebyte.enum import DBVarType
from featurebyte.query_graph.model.column_info import ColumnSpecWithDescription
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.schema.table import TableCreate


class BaseApiTestSuite:
    """
    BaseApiTestSuite contains common api tests
    """

    # class variables to be set at metaclass
    base_route = None
    class_name = None
    payload = None
    async_create = False
    time_limit = 10
    wrap_payload_on_create = False
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []
    list_unprocessable_params_expected_detail_pairs = [
        (
            {"page_size": 0},
            [
                {
                    "ctx": {"gt": 0},
                    "input": "0",
                    "loc": ["query", "page_size"],
                    "msg": "Input should be greater than 0",
                    "type": "greater_than",
                }
            ],
        ),
        (
            {"page_size": 501},
            [
                {
                    "ctx": {"le": 500},
                    "input": "501",
                    "loc": ["query", "page_size"],
                    "msg": "Input should be less than or equal to 500",
                    "type": "less_than_equal",
                }
            ],
        ),
        (
            {"page_size": "abcd"},
            [
                {
                    "input": "abcd",
                    "loc": ["query", "page_size"],
                    "msg": "Input should be a valid integer, unable to parse string as an "
                    "integer",
                    "type": "int_parsing",
                }
            ],
        ),
        (
            {"sort_by": "", "search": ""},
            [
                {
                    "ctx": {"min_length": 1},
                    "input": "",
                    "loc": ["query", "sort_by"],
                    "msg": "String should have at least 1 character",
                    "type": "string_too_short",
                },
                {
                    "ctx": {"min_length": 1},
                    "input": "",
                    "loc": ["query", "search"],
                    "msg": "String should have at least 1 character",
                    "type": "string_too_short",
                },
            ],
        ),
        (
            {"sort_dir": "abcd"},
            [
                {
                    "ctx": {"expected": "'asc' or 'desc'"},
                    "input": "abcd",
                    "loc": ["query", "sort_dir"],
                    "msg": "Input should be 'asc' or 'desc'",
                    "type": "literal_error",
                }
            ],
        ),
    ]

    @property
    def class_name_to_save(self):
        """Class name used to save the object"""
        return self.class_name

    @staticmethod
    def load_payload(filename):
        """Load payload"""
        with open(filename) as fhandle:
            return json.loads(fhandle.read())

    @property
    def id_field_name(self):
        """ID field name in the url"""
        base_name = self.base_route.lstrip("/")
        return f"{base_name}_id"

    def pytest_generate_tests(self, metafunc):
        """Parametrize fixture at runtime"""
        if "create_conflict_payload_expected_detail" in metafunc.fixturenames:
            metafunc.parametrize(
                "create_conflict_payload_expected_detail",
                self.create_conflict_payload_expected_detail_pairs,
            )
        if "create_unprocessable_payload_expected_detail" in metafunc.fixturenames:
            metafunc.parametrize(
                "create_unprocessable_payload_expected_detail",
                self.create_unprocessable_payload_expected_detail_pairs,
            )

        if "list_unprocessable_params_expected_detail" in metafunc.fixturenames:
            metafunc.parametrize(
                "list_unprocessable_params_expected_detail",
                self.list_unprocessable_params_expected_detail_pairs,
            )

    @pytest.fixture(autouse=True)
    def mock_detect_and_update_column_dtypes(self):
        """Mock columns attributes service execution"""
        with patch(
            "featurebyte.service.specialized_dtype.SpecializedDtypeDetectionService.detect_and_update_column_dtypes"
        ):
            yield

    @pytest.fixture(name="mock_deployment_flow")
    def mock_deployment_flow_fixture(self, mock_deployment_flow):
        """Mock deployment flow fixture"""
        with patch(
            "featurebyte.service.offline_store_feature_table_manager.OfflineStoreFeatureTableManagerService."
            "handle_online_enabled_features"
        ):
            yield mock_deployment_flow

    @staticmethod
    def make_feature_production_ready(api_client, feature_id, catalog_id):
        """Make feature production ready"""
        response = api_client.patch(
            f"/feature/{feature_id}",
            headers={"active-catalog-id": str(catalog_id)},
            json={"readiness": "PRODUCTION_READY"},
        )
        assert response.status_code == HTTPStatus.OK, response.json()

    @staticmethod
    def tag_table_entity(api_client, table_type, table_payload):
        """Tag table entity"""
        if table_type not in {"item_table", "event_table", "dimension_table", "scd_table"}:
            return

        table_id = table_payload["_id"]
        for column in table_payload["columns_info"]:
            if column["entity_id"]:
                response = api_client.patch(
                    f"/{table_type}/{table_id}/column_entity",
                    json={"column_name": column["name"], "entity_id": column["entity_id"]},
                )
                assert response.status_code == HTTPStatus.OK, response.json()

    def update_deployment_enabled(self, api_client, deployment_id, catalog_id, enabled=True):
        """Enable deployment"""
        response = api_client.patch(
            f"/deployment/{deployment_id}",
            headers={"active-catalog-id": str(catalog_id)},
            json={"enabled": enabled},
        )
        assert response.status_code == HTTPStatus.ACCEPTED, response.json()
        self.wait_for_results(api_client, response)
        deployment_response = api_client.get(
            f"/deployment/{deployment_id}",
            headers={"active-catalog-id": str(catalog_id)},
        )
        assert deployment_response.status_code == HTTPStatus.OK
        assert deployment_response.json()["enabled"] == enabled

    def wait_for_results(self, api_client, create_response):
        """
        Wait for async job to complete
        """
        task_submission = create_response.json()
        if not create_response.is_success:
            raise AssertionError(f'Task submission failed: {task_submission["detail"]}')
        task_id = task_submission["id"]

        start_time = datetime.now()
        while (datetime.now() - start_time).seconds < self.time_limit:
            response = api_client.get(f"/task/{task_id}")
            task_status = response.json()
            status = task_status["status"]
            if status not in ["PENDING", "RECEIVED", "STARTED"]:
                if status in ["SUCCESS", "FAILURE"]:
                    break
            sleep(0.1)
        return response

    def setup_creation_route(self, api_client):
        """Setup for post route"""

    def post(self, api_client, payload, **kwargs):
        """Call post route with payload"""

        if self.wrap_payload_on_create:
            # When set, the payload is passed via data instead of json since the route expects
            # multipart/form-data handling. Because of that, the payload is also wrapped in this
            # format: {"payload": payload_in_json}.
            data = {"payload": json.dumps(payload)}
            return api_client.post(self.base_route, data=data, **kwargs)

        return api_client.post(self.base_route, json=payload, **kwargs)

    def activate_catalog(self, api_client):
        """Create and activate catalog"""
        # create feature store
        payload = self.load_payload("tests/fixtures/request_payloads/feature_store.json")
        api_client.post("/feature_store", json=payload)

        # create catalog
        payload = self.load_payload("tests/fixtures/request_payloads/catalog.json")
        response = api_client.post("/catalog", json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        catalog_id = response.json()["_id"]

        api_client.headers["active-catalog-id"] = catalog_id

    @pytest.fixture()
    def test_api_client_persistent(self, api_client_persistent):
        """Test api client with persistent headers"""
        return api_client_persistent

    @pytest_asyncio.fixture()
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        id_before = self.payload["_id"]
        response = self.post(test_api_client, self.payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED, response_dict
        assert response_dict["_id"] == id_before

        if "_table" in self.base_route:
            self.tag_table_entity(test_api_client, self.base_route.strip("/"), self.payload)
        return response

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        return []

    def create_multiple_success_responses_post_processing(self, api_client):
        """Post multiple success responses"""
        _ = api_client

    @pytest_asyncio.fixture()
    async def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        output = []
        for _, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
            response = self.post(test_api_client, payload)
            assert response.status_code == HTTPStatus.CREATED, response.json()
            if self.async_create:
                assert response.json()["status"] == "SUCCESS"
            else:
                assert response.json()["_id"] == payload["_id"]
            output.append(response)

        # post-processing after all the responses are created
        self.create_multiple_success_responses_post_processing(test_api_client)
        return output

    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        """Test creation (success) without specifying id field"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = {key: value for key, value in self.payload.items() if key != "_id"}
        assert "_id" not in payload
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.CREATED

    def test_create_201__id_is_none(self, test_api_client_persistent):
        """Test creation (success) ID is None"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = self.payload.copy()
        payload["_id"] = None
        response = self.post(test_api_client, payload)
        assert response.status_code == HTTPStatus.CREATED

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        assert create_success_response.status_code == HTTPStatus.CREATED
        result = create_success_response.json()

        # check response
        doc_id = ObjectId(result["_id"])
        assert result["user_id"] == str(user_id)
        assert datetime.fromisoformat(result["created_at"]) < datetime.utcnow()

        # test get audit record
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}/audit/{doc_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["data"][-1]["action_type"] == "INSERT"
        assert response_dict["data"][-1]["previous_values"] == {}

    def test_create_409(
        self,
        test_api_client_persistent,
        create_success_response,
        create_conflict_payload_expected_detail,
    ):
        """Test creation (conflict)"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent

        conflict_payload, expected_message = create_conflict_payload_expected_detail
        response = self.post(test_api_client, conflict_payload)
        assert response.status_code == HTTPStatus.CONFLICT
        assert response.json()["detail"] == expected_message

    def test_create_422(
        self,
        test_api_client_persistent,
        create_success_response,
        create_unprocessable_payload_expected_detail,
    ):
        """Test creation (unprocessable entity)"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent
        unprocessable_payload, expected_detail = create_unprocessable_payload_expected_detail
        response = self.post(test_api_client, unprocessable_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == expected_detail

    def test_get_200(self, test_api_client_persistent, create_success_response, user_id):
        """Test get (success)"""
        test_api_client, _ = test_api_client_persistent
        create_response_dict = create_success_response.json()
        doc_id = create_response_dict["_id"]

        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["_id"] == doc_id
        assert datetime.fromisoformat(response_dict["created_at"]) < datetime.utcnow()
        assert response_dict["user_id"] == str(user_id)
        assert response_dict["name"] == self.payload["name"]

    def test_get_404(self, test_api_client_persistent):
        """Test get (not found)"""
        test_api_client, _ = test_api_client_persistent
        unknown_id = ObjectId()
        response = test_api_client.get(f"{self.base_route}/{unknown_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND
        error_message = (
            f'{self.class_name} (id: "{unknown_id}") not found.'
            f" Please save the {self.class_name_to_save} object first."
        )
        assert response.json()["detail"] == error_message

    def test_get_422(self, test_api_client_persistent):
        """Test get (unprocessable)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}/abcd")
        response_detail = response.json()["detail"]
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response_detail == [
            {
                "ctx": {"class": "ObjectId"},
                "input": "abcd",
                "loc": response_detail[0]["loc"],
                "msg": "Input should be an instance of ObjectId",
                "type": "is_instance_of",
            },
            {
                "ctx": {"error": {}},
                "input": "abcd",
                "loc": response_detail[1]["loc"],
                "msg": "Value error, Invalid ObjectId",
                "type": "value_error",
            },
        ]

    def test_list_200(self, test_api_client_persistent, create_multiple_success_responses):
        """Test list (success, multiple)"""
        # test with default params
        test_api_client, _ = test_api_client_persistent
        _ = create_multiple_success_responses
        response = test_api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        expected_paginated_info = {"page": 1, "page_size": 10, "total": 3}

        assert len(response_dict["data"]) == 3
        assert response_dict.items() >= expected_paginated_info.items()
        expected_names = [
            payload["name"]
            for payload in self.multiple_success_payload_generator(api_client=test_api_client)
        ]
        response_data_names = [elem["name"] for elem in response_dict["data"]]
        expected_names = list(reversed(expected_names))
        assert response_data_names == expected_names

        # test with pagination parameters (page 1)
        response_with_params = test_api_client.get(
            f"{self.base_route}",
            params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 1},
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_dict = response_with_params.json()
        expected_paginated_info = {"page": 1, "page_size": 2, "total": 3}

        assert response_with_params_dict.items() >= expected_paginated_info.items()
        response_with_params_names = [elem["name"] for elem in response_with_params_dict["data"]]
        expected_sorted_names = sorted(expected_names)
        assert response_with_params_names == expected_sorted_names[:2]

        # test with pagination parameters (page 2)
        response_with_params = test_api_client.get(
            f"{self.base_route}",
            params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 2},
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_dict = response_with_params.json()
        assert response_with_params_dict.items() >= {**expected_paginated_info, "page": 2}.items()
        response_with_params_names = [elem["name"] for elem in response_with_params_dict["data"]]
        assert response_with_params_names == expected_sorted_names[-1:]

        # test sort_by with some random unknown column name
        # should not throw error, just that the sort_by param has no real effect since column not found
        response_with_params = test_api_client.get(
            f"{self.base_route}", params={"sort_by": "random_name"}
        )
        assert response_with_params.status_code == HTTPStatus.OK

        # test name parameter
        response_with_params = test_api_client.get(
            f"{self.base_route}", params={"name": expected_names[1]}
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_names = [elem["name"] for elem in response_with_params.json()["data"]]
        assert response_with_params_names == [expected_names[1]]

        # test bench_size_boundary
        response_page_size_boundary = test_api_client.get(
            f"{self.base_route}", params={"page_size": 100}
        )
        assert response_page_size_boundary.status_code == HTTPStatus.OK

    def test_list_422(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
        list_unprocessable_params_expected_detail,
    ):
        """Test list (unprocessable)"""
        test_api_client, _ = test_api_client_persistent
        _ = create_multiple_success_responses
        unprocessable_params, expected_detail = list_unprocessable_params_expected_detail
        response = test_api_client.get(f"{self.base_route}", params=unprocessable_params)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == expected_detail

    def test_list_501(self, test_api_client_persistent, create_success_response):
        """Test list (not implemented)"""
        _ = create_success_response
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}", params={"search": "abc"})
        assert response.status_code == HTTPStatus.NOT_IMPLEMENTED
        assert response.json()["detail"] == "Query not supported."

    def test_list_audit_422(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
        list_unprocessable_params_expected_detail,
    ):
        """Test list audit (unprocessable)"""
        test_api_client, _ = test_api_client_persistent
        _ = create_multiple_success_responses
        unprocessable_params, expected_detail = list_unprocessable_params_expected_detail
        response = test_api_client.get(
            f"{self.base_route}/audit/{ObjectId()}", params=unprocessable_params
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == expected_detail

    def test_list_audit_422__invalid_id_value(self, test_api_client_persistent):
        """Test list audit (unprocessable) - invalid id value"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}/audit/abc")
        response_detail = response.json()["detail"]
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "ctx": {"class": "ObjectId"},
                "input": "abc",
                "loc": response_detail[0]["loc"],
                "msg": "Input should be an instance of ObjectId",
                "type": "is_instance_of",
            },
            {
                "ctx": {"error": {}},
                "input": "abc",
                "loc": response_detail[1]["loc"],
                "msg": "Value error, Invalid ObjectId",
                "type": "value_error",
            },
        ]

    def test_update_description_200(self, test_api_client_persistent, create_success_response):
        """
        Test update description
        """
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}/description",
            json={
                "description": "new description",
            },
        )
        assert response.status_code == HTTPStatus.OK, response.json()
        assert response.json()["description"] == "new description"

        # setting description to None should clear it
        response = test_api_client.patch(
            f"{self.base_route}/{doc_id}/description",
            json={
                "description": None,
            },
        )
        assert response.status_code == HTTPStatus.OK
        assert response.json()["description"] is None


class BaseAsyncApiTestSuite(BaseApiTestSuite):
    """
    BaseAsyncApiTestSuite contains common api tests with async creation routes
    """

    async_create = True

    @pytest.fixture()
    def test_api_client_persistent(self, api_client_persistent):
        """Test api client with persistent headers"""
        api_client, _ = api_client_persistent
        self.activate_catalog(api_client)
        return api_client_persistent

    @pytest.fixture()
    def default_catalog_id(self, test_api_client_persistent):
        """Get default catalog id for testing"""
        api_client, _ = test_api_client_persistent
        return api_client.headers["active-catalog-id"]

    @pytest_asyncio.fixture()
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        id_before = self.payload["_id"]
        response = self.post(test_api_client, self.payload)

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS", response_dict["traceback"]

        response = test_api_client.get(response_dict["output_path"])
        response_dict = response.json()
        assert response_dict["_id"] == id_before
        return response

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        assert create_success_response.status_code == HTTPStatus.OK
        result = create_success_response.json()

        # check response
        doc_id = ObjectId(result["_id"])
        assert result["user_id"] == str(user_id)
        assert datetime.fromisoformat(result["created_at"]) < datetime.utcnow()
        assert result["updated_at"] is None

        # test get audit record
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}/audit/{doc_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["total"] == 1
        assert [record["action_type"] for record in response_dict["data"]] == ["INSERT"]
        assert [record["previous_values"] for record in response_dict["data"]] == [{}]


class BaseCatalogApiTestSuite(BaseApiTestSuite):
    """
    BaseCatalogApiTestSuite includes some checks for behaviour of objects that belong to catalogs
    """

    @pytest.fixture()
    def default_catalog_id(self, test_api_client_persistent):
        """Get default catalog id for testing"""
        api_client, _ = test_api_client_persistent
        return api_client.headers["active-catalog-id"]

    @pytest.fixture(name="catalog_id")
    def catalog_id_fixture(self, test_api_client_persistent):
        """
        Create catalog
        """
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post(
            "/catalog", json={"name": "Test", "default_feature_store_ids": []}
        )
        assert response.status_code == HTTPStatus.CREATED
        return ObjectId(response.json()["_id"])

    @pytest.fixture()
    def test_api_client_persistent(self, api_client_persistent):
        """Test api client with persistent headers"""
        api_client, _ = api_client_persistent
        self.activate_catalog(api_client)
        return api_client_persistent

    @staticmethod
    def create_new_feature_version(test_api_client, feature_id):
        """Create new feature version"""
        post_feature_response = test_api_client.post(
            "/feature",
            json={
                "source_feature_id": feature_id,
                "table_feature_job_settings": [
                    {
                        "table_name": "sf_event_table",
                        "feature_job_setting": {
                            "blind_spot": "23h",
                            "period": "24h",
                            "offset": "1h",
                        },
                    }
                ],
            },
        )
        assert post_feature_response.status_code == HTTPStatus.CREATED
        new_feature_id = post_feature_response.json()["_id"]
        return new_feature_id

    def test_list_no_catalog_424(self, api_client_persistent):
        """
        Test list when no catalog is active
        """
        api_client, _ = api_client_persistent
        response = api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.FAILED_DEPENDENCY


class BaseRelationshipApiTestSuite(BaseApiTestSuite):
    """
    BaseRelationshipApiTestSuite contains tests related to adding & removing parent object
    """

    unknown_id = ObjectId()
    create_parent_unprocessable_payload_expected_detail_pairs = []

    def pytest_generate_tests(self, metafunc):
        """Parametrize fixture at runtime"""
        super().pytest_generate_tests(metafunc)
        if "create_parent_unprocessable_payload_expected_detail" in metafunc.fixturenames:
            metafunc.parametrize(
                "create_parent_unprocessable_payload_expected_detail",
                self.create_parent_unprocessable_payload_expected_detail_pairs,
            )

    @staticmethod
    def prepare_parent_payload(payload):
        """Prepare payload to create parent relationship"""
        return payload

    def test_create_201_and_delete_parent_200(
        self, test_api_client_persistent, create_multiple_success_responses
    ):
        """
        Test create parent & child relationship
        """
        test_api_client, _ = test_api_client_persistent

        parent_id = create_multiple_success_responses[0].json()["_id"]
        child_response_dict = create_multiple_success_responses[1].json()
        child_id = child_response_dict["_id"]

        # create parent relationship
        parent = self.prepare_parent_payload({"id": parent_id})
        response = test_api_client.post(f"{self.base_route}/{child_id}/parent", json=parent)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED
        assert (
            response_dict.items()
            >= {
                "_id": child_id,
                "user_id": response_dict["user_id"],
                "name": child_response_dict["name"],
                "created_at": child_response_dict["created_at"],
                "updated_at": response_dict["updated_at"],
                "ancestor_ids": [parent_id],
                "parents": [parent],
            }.items()
        )

        # remove parent relationship
        response = test_api_client.delete(f"{self.base_route}/{child_id}/parent/{parent_id}")
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert (
            response_dict.items()
            >= {
                "_id": child_id,
                "user_id": response_dict["user_id"],
                "name": child_response_dict["name"],
                "created_at": child_response_dict["created_at"],
                "updated_at": response_dict["updated_at"],
                "ancestor_ids": [],
                "parents": [],
            }.items()
        )

    def test_create_parent_422(
        self,
        create_success_response,
        test_api_client_persistent,
        create_parent_unprocessable_payload_expected_detail,
    ):
        """
        Test create parent with non-existence parent ID
        """
        create_success_response_dict = create_success_response.json()
        test_api_client, _ = test_api_client_persistent

        (
            unprocessible_entity_payload,
            expected_message,
        ) = create_parent_unprocessable_payload_expected_detail
        response = test_api_client.post(
            f"{self.base_route}/{create_success_response_dict['_id']}/parent",
            json=unprocessible_entity_payload,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == expected_message

    def test_create_parent_422__child_not_found(
        self, test_api_client_persistent, create_success_response
    ):
        """
        Test create parent with non-existent child ID
        """
        test_api_client, _ = test_api_client_persistent
        create_success_response_dict = create_success_response.json()
        unknown_id = ObjectId()
        response = test_api_client.post(
            f"{self.base_route}/{unknown_id}/parent",
            json=self.prepare_parent_payload({"id": create_success_response_dict["_id"]}),
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        expected = f'{self.class_name} (id: "{unknown_id}") not found. Please save the {self.class_name} object first.'
        assert response.json()["detail"] == expected

    def test_create_parent_422__both_parent_and_child(
        self, create_success_response, test_api_client_persistent
    ):
        """
        Test create parent (unprocessible entity) when parent & child are the same ID
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        parent = self.prepare_parent_payload({"id": str(response_dict["_id"])})
        response = test_api_client.post(
            f"{self.base_route}/{response_dict['_id']}/parent", json=parent
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": f'Object "{response_dict["name"]}" cannot be both parent & child.'
        }

    def test_delete_parent_422__when_id_is_not_a_valid_parent(
        self, create_success_response, test_api_client_persistent
    ):
        """
        Test delete parent (unprocessible entity) when the given parent ID is not a valid parent
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = create_success_response.json()
        response = test_api_client.delete(
            f"{self.base_route}/{response_dict['_id']}/parent/{response_dict['_id']}",
        )
        name = response_dict["name"]
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": f'Object "{name}" is not the parent of object "{name}".'
        }


class BaseCatalogRelationshipApiTestSuite(BaseRelationshipApiTestSuite, BaseCatalogApiTestSuite):
    """
    BaseCatalogRelationshipApiTestSuite contains tests related to adding & removing parent object
    for catalog-specific objects
    """


class BaseTableApiTestSuite(BaseCatalogApiTestSuite):
    """
    BaseTableApiTestSuite contains tests related to table service
    """

    data_create_schema_class = TableCreate
    update_unprocessable_payload_expected_detail_pairs = []

    def pytest_generate_tests(self, metafunc):
        """Parametrize fixture at runtime"""
        base_update_unprocessable_payload_expected_detail_pairs = [
            (
                {"record_creation_timestamp_column": "non-exist-columns"},
                (
                    f"1 validation error for {self.class_name}Model\n"
                    '  Value error, Column "non-exist-columns" not found in the table!'
                ),
            ),
            (
                {"record_creation_timestamp_column": "item_id"},
                (
                    f"1 validation error for {self.class_name}Model\n"
                    f"  Value error, Column \"item_id\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ'] "
                ),
            ),
        ]

        super().pytest_generate_tests(metafunc)
        if "update_unprocessable_payload_expected_detail" in metafunc.fixturenames:
            metafunc.parametrize(
                "update_unprocessable_payload_expected_detail",
                (
                    base_update_unprocessable_payload_expected_detail_pairs
                    + self.update_unprocessable_payload_expected_detail_pairs
                ),
            )

    def setup_creation_route(self, api_client):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("entity", "entity"),
            ("entity", "entity_transaction"),
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
            payload["name"] = f'{self.payload["name"]}_{i}'
            tabular_source = payload["tabular_source"]
            payload["tabular_source"] = {
                "feature_store_id": tabular_source["feature_store_id"],
                "table_details": {
                    key: f"{value}_{i}" for key, value in tabular_source["table_details"].items()
                },
            }
            yield payload

    @pytest.fixture(name="tabular_source")
    def tabular_source_fixture(self, snowflake_feature_store):
        """Fixture for tabular source"""
        return {
            "feature_store_id": str(snowflake_feature_store.id),
            "table_details": {
                "database_name": "database",
                "schema_name": "schema",
                "table_name": "table",
            },
        }

    @pytest.fixture(name="feature_store_details")
    def feature_store_details_fixture(self, snowflake_feature_store):
        """Fixture for feature store details"""
        return FeatureStoreDetails(**snowflake_feature_store.model_dump())

    @pytest.fixture(name="columns_info")
    def column_info_fixture(self):
        """Fixture for columns info"""
        return [
            {"name": "created_at", "dtype": "TIMESTAMP", "entity_id": None, "semantic_id": None},
            {"name": "effective_at", "dtype": "TIMESTAMP", "entity_id": None, "semantic_id": None},
            {"name": "end_at", "dtype": "TIMESTAMP", "entity_id": None, "semantic_id": None},
            {
                "name": "another_created_at",
                "dtype": "TIMESTAMP",
                "entity_id": None,
                "semantic_id": None,
            },
            {"name": "event_date", "dtype": "TIMESTAMP", "entity_id": None, "semantic_id": None},
            {"name": "date", "dtype": "VARCHAR", "entity_id": None, "semantic_id": None},
            {"name": "event_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
            {"name": "series_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
            {"name": "dimension_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
            {"name": "surrogate_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
            {"name": "natural_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
            {"name": "current_value", "dtype": "BOOL", "entity_id": None, "semantic_id": None},
            {"name": "item_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
        ]

    @pytest.fixture(name="data_response")
    def data_response_fixture(
        self, test_api_client_persistent, data_model_dict, columns_info, snowflake_feature_store
    ):
        """
        Event table response fixture
        """
        _ = snowflake_feature_store
        test_api_client, _ = test_api_client_persistent
        payload = self.data_create_schema_class(**data_model_dict).json_dict()
        payload["columns_info"] = columns_info
        response = test_api_client.post(self.base_route, json=payload)
        assert response.status_code == HTTPStatus.CREATED, response.json()
        assert response.json()["_id"] == data_model_dict["_id"]
        return response

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        # description is stored during creation
        response_dict = create_success_response.json()
        assert response_dict["description"] == self.payload["description"]
        assert response_dict["status"] == "PUBLIC_DRAFT"

    def test_create_422_invalid_special_columns(self, test_api_client_persistent):
        """Test creation (unprocessable) when special columns are invalid"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = self.payload.copy()

        # set special columns to invalid values
        special_columns = [
            field_name
            for field_name in self.data_create_schema_class.model_fields
            if field_name.endswith("column") and field_name != "datetime_partition_column"
        ]
        for special_column in special_columns:
            payload[special_column] = ""

        response = self.post(test_api_client, payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response_dict == {
            "detail": [
                {
                    "ctx": {"error": {}},
                    "input": "",
                    "loc": ["body", special_column],
                    "msg": "Value error, Column not found in table: ",
                    "type": "value_error",
                }
                for special_column in special_columns
            ]
        }

    def test_update_fails_table_not_found(self, test_api_client_persistent, data_update_dict):
        """
        Update Data fails if table not found
        """
        test_api_client, _ = test_api_client_persistent
        random_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{random_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()
        assert response.json() == {
            "detail": (
                f'{self.class_name} (id: "{random_id}") not found. '
                f"Please save the {self.class_name} object first."
            )
        }

    def test_update_fails_invalid_transition(
        self, test_api_client_persistent, data_response, data_update_dict
    ):
        """
        Update Data fails if status transition is no valid
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        data_update_dict["status"] = "DEPRECATED"
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}", json=data_update_dict
        )
        assert response.status_code == HTTPStatus.OK

        # try to update to PUBLIC_DRAFT from DEPRECATED
        data_update_dict["status"] = "PUBLIC_DRAFT"
        response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}", json=data_update_dict
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {
            "detail": "Invalid status transition from DEPRECATED to PUBLIC_DRAFT."
        }

    def test_update_status_only(self, test_api_client_persistent, data_response):
        """
        Update Tabular Data status only
        """
        # insert a record
        test_api_client, _ = test_api_client_persistent
        current_data = data_response.json()
        assert current_data.pop("status") == "PUBLIC_DRAFT"
        assert current_data.pop("updated_at") is not None

        response = test_api_client.patch(
            f"{self.base_route}/{current_data['_id']}",
            json={"status": "PUBLISHED"},
        )
        assert response.status_code == HTTPStatus.OK
        updated_data = response.json()
        updated_at = datetime.fromisoformat(updated_data.pop("updated_at"))
        assert updated_at > datetime.fromisoformat(updated_data["created_at"])

        # expect status to be published
        assert updated_data.pop("status") == "PUBLISHED"

        # the other fields should be unchanged
        assert updated_data == current_data

        # test get audit records
        response = test_api_client.get(f"{self.base_route}/audit/{current_data['_id']}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 4
        assert [record["action_type"] for record in results["data"]] == [
            "UPDATE",
            "UPDATE",
            "UPDATE",
            "INSERT",
        ]
        assert [record["previous_values"].get("status") for record in results["data"]] == [
            "PUBLIC_DRAFT",
            None,
            None,
            None,
        ]

    def test_update_422__invalid_id_value(self, test_api_client_persistent, data_update_dict):
        """Test update (unprocessable) - invalid id value"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.patch(f"{self.base_route}/abc", json=data_update_dict)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "ctx": {"class": "ObjectId"},
                "input": "abc",
                "loc": ["path", self.id_field_name, "is-instance[ObjectId]"],
                "msg": "Input should be an instance of ObjectId",
                "type": "is_instance_of",
            },
            {
                "ctx": {"error": {}},
                "input": "abc",
                "loc": ["path", self.id_field_name, "function-after[validate(), str]"],
                "msg": "Value error, Invalid ObjectId",
                "type": "value_error",
            },
        ]

    def test_update_column_entity_422__entity_id_not_found(
        self, test_api_client_persistent, data_response
    ):
        """Test update column entity (unprocessable) - entity ID not found"""
        test_api_client, _ = test_api_client_persistent
        data_response_dict = data_response.json()

        unknown_entity_id = str(ObjectId())
        response = test_api_client.patch(
            f"{self.base_route}/{data_response_dict['_id']}/column_entity",
            json={
                "column_name": "item_id",
                "entity_id": unknown_entity_id,
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == (
            f"Entity IDs ['{unknown_entity_id}'] not found for columns ['item_id']."
        )

    def test_update_record_creation_date(
        self,
        test_api_client_persistent,
        data_response,
    ):
        """
        Update Event Data record creation timestamp column
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()

        update_response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}",
            json={"record_creation_timestamp_column": "another_created_at"},
        )
        update_response_dict = update_response.json()
        expected_response = {
            **response_dict,
            "record_creation_timestamp_column": "another_created_at",
        }
        expected_response.pop("updated_at")
        assert update_response_dict.items() > expected_response.items()
        assert update_response_dict["updated_at"] is not None

    def test_update_column_critical_data_info_old(
        self, test_api_client_persistent, data_response, columns_info
    ):
        """
        Test update columns info using column update route (DEPRECATED).

        Update of columns info is deprecated and will be removed in release 0.5.0
        See https://featurebyte.atlassian.net/browse/DEV-2000
        """

        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()

        # modify current_value's critical data info
        current_value_info = columns_info[-2]
        assert current_value_info["name"] == "current_value"
        current_value_info["critical_data_info"] = {
            "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
        }
        columns_info[-2] = current_value_info

        # update critical data info
        update_response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}",
            json={"columns_info": columns_info},
        )
        assert update_response.status_code == HTTPStatus.OK, update_response.json()
        output_dict = update_response.json()
        assert output_dict["columns_info"][-2].get("critical_data_info") == {
            "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
        }

    def test_update_column_critical_data_info(
        self, test_api_client_persistent, data_response, columns_info
    ):
        """Test update column critical_data_info"""
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()

        # modify current_value's critical data info
        current_value_info = columns_info[-2]
        assert current_value_info["name"] == "current_value"
        assert current_value_info.get("critical_data_info") is None

        # update critical data info
        update_response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}/column_critical_data_info",
            json={
                "column_name": "current_value",
                "critical_data_info": {
                    "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
                },
            },
        )
        assert update_response.status_code == HTTPStatus.OK
        output_dict = update_response.json()
        assert output_dict["columns_info"][-2].get("critical_data_info") == {
            "cleaning_operations": [{"type": "missing", "imputed_value": 0}]
        }

    def test_update_column_description_404__column_not_found(
        self, test_api_client_persistent, data_response
    ):
        """Test update column description"""
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        document_id = response_dict["_id"]

        # update description in a column that does not exist
        update_response = test_api_client.patch(
            f"{self.base_route}/{document_id}/column_description",
            json={
                "column_name": "non_existent_column",
                "description": "new description",
            },
        )
        assert update_response.status_code == HTTPStatus.NOT_FOUND
        assert update_response.json()["detail"] == (
            f'Column: non_existent_column not found in {self.class_name_to_save} (id: "{document_id}")'
        )

    def test_update_column_description(
        self, test_api_client_persistent, data_response, columns_info
    ):
        """Test update column description"""
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()

        # modify current_value's description
        current_value_info = columns_info[-2]
        assert current_value_info["name"] == "current_value"
        assert current_value_info.get("description") is None

        # update description
        update_response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}/column_description",
            json={
                "column_name": "current_value",
                "description": "new description",
            },
        )
        assert update_response.status_code == HTTPStatus.OK, update_response.json()
        output_dict = update_response.json()
        assert output_dict["columns_info"][-2].get("description") == "new description"

        update_response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}/column_description",
            json={
                "column_name": "current_value",
                "description": None,
            },
        )
        assert update_response.status_code == HTTPStatus.OK
        output_dict = update_response.json()
        assert output_dict["columns_info"][-2].get("description") is None

    def test_update_422(
        self,
        data_response,
        test_api_client_persistent,
        update_unprocessable_payload_expected_detail,
    ):
        """
        Test Update (unprocessible entity)
        """
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        insert_id = response_dict["_id"]

        (
            unprocessible_entity_payload,
            expected_message,
        ) = update_unprocessable_payload_expected_detail
        response = test_api_client.patch(
            f"{self.base_route}/{insert_id}",
            json=unprocessible_entity_payload,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert expected_message in response.json()["detail"]

    def test_table_get_200(self, test_api_client_persistent, create_success_response):
        """Test table get (success)"""
        test_api_client, _ = test_api_client_persistent
        success_response_dict = create_success_response.json()

        # check that table route can be used to retrieve the created table
        response = test_api_client.get(f"/table/{success_response_dict['_id']}")
        response_dict = response.json()

        # overwrite the updated_at & columns_info fields (due to entity tagging)
        success_response_dict["updated_at"] = response_dict["updated_at"]
        success_response_dict["columns_info"] = response_dict["columns_info"]
        assert response_dict == success_response_dict

    def test_table_list_200(self, test_api_client_persistent, create_multiple_success_responses):
        """Test table list (success, multiple)"""
        # test with default params
        test_api_client, _ = test_api_client_persistent
        _ = create_multiple_success_responses
        response = test_api_client.get(self.base_route)
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        expected_paginated_info = {"page": 1, "page_size": 10, "total": 3}

        assert len(response_dict["data"]) == 3
        assert response_dict.items() >= expected_paginated_info.items()
        expected_names = [
            payload["name"]
            for payload in self.multiple_success_payload_generator(api_client=test_api_client)
        ]
        response_data_names = [elem["name"] for elem in response_dict["data"]]
        expected_names = list(reversed(expected_names))
        assert response_data_names == expected_names

        # test with pagination parameters (page 1)
        response_with_params = test_api_client.get(
            self.base_route,
            params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 1},
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_dict = response_with_params.json()
        expected_paginated_info = {"page": 1, "page_size": 2, "total": 3}

        assert response_with_params_dict.items() >= expected_paginated_info.items()
        response_with_params_names = [elem["name"] for elem in response_with_params_dict["data"]]
        expected_sorted_names = sorted(expected_names)
        assert response_with_params_names == expected_sorted_names[:2]

        # test with pagination parameters (page 2)
        response_with_params = test_api_client.get(
            self.base_route,
            params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 2},
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_dict = response_with_params.json()
        assert response_with_params_dict.items() >= {**expected_paginated_info, "page": 2}.items()
        response_with_params_names = [elem["name"] for elem in response_with_params_dict["data"]]
        assert response_with_params_names == expected_sorted_names[-1:]

        # test sort_by with some random unknown column name
        # should not throw error, just that the sort_by param has no real effect since column not found
        response_with_params = test_api_client.get(
            self.base_route, params={"sort_by": "random_name"}
        )
        assert response_with_params.status_code == HTTPStatus.OK

        # test name parameter
        response_with_params = test_api_client.get(
            self.base_route, params={"name": expected_names[1]}
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_names = [elem["name"] for elem in response_with_params.json()["data"]]
        assert response_with_params_names == [expected_names[1]]

        # test bench_size_boundary
        response_page_size_boundary = test_api_client.get(
            self.base_route, params={"page_size": 100}
        )
        assert response_page_size_boundary.status_code == HTTPStatus.OK


class BaseMaterializedTableTestSuite(BaseAsyncApiTestSuite):
    """
    Base test suite for materialized table which includes tests for delete materialized table
    """

    def test_delete_200(self, test_api_client_persistent, create_success_response):
        """Test delete route (success)"""
        test_api_client, _ = test_api_client_persistent
        doc_id = create_success_response.json()["_id"]
        response = test_api_client.delete(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.ACCEPTED, response.json()

        # check that the task is completed with success
        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK, response_dict
        assert response_dict["status"] == "SUCCESS", response_dict

        # check that the table is deleted
        response = test_api_client.get(f"{self.base_route}/{doc_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_delete_404(self, test_api_client_persistent):
        """Test delete route (404)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.delete(f"{self.base_route}/{str(ObjectId())}")
        assert response.status_code == HTTPStatus.NOT_FOUND, response.json()

    def test_download_422(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """Test download (failed)"""
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.OK
        result = create_success_response.json()
        doc_id = result["_id"]

        expected_df = pd.DataFrame({"colA": [1, 2, 3]})

        async def mock_get_async_query_stream(query):
            _ = query
            yield dataframe_to_arrow_bytes(expected_df)

        mock_session = mock_get_session.return_value
        mock_session.get_async_query_stream = Mock(side_effect=mock_get_async_query_stream)
        mock_session.execute_query.return_value = pd.DataFrame({"row_count": [301 * 10000000]})
        mock_session.list_table_schema.return_value = {
            "colA": ColumnSpecWithDescription(name="colA", dtype=DBVarType.INT)
        }
        mock_session.generate_session_unique_id = Mock(return_value="1")

        response = test_api_client.get(f"{self.base_route}/pyarrow_table/{doc_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json() == {"detail": "Table size (3010000000, 1) exceeds download limit."}

    def test_download(self, test_api_client_persistent, create_success_response, mock_get_session):
        """Test download (success)"""
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.OK
        result = create_success_response.json()
        doc_id = result["_id"]
        table_name = result["location"]["table_details"]["table_name"]

        expected_df = pd.DataFrame({"colA": [1, 2, 3]})

        async def mock_get_async_query_stream(query):
            _ = query
            yield dataframe_to_arrow_bytes(expected_df)

        mock_session = mock_get_session.return_value
        mock_session.get_async_query_stream = Mock(side_effect=mock_get_async_query_stream)
        mock_session.execute_query.return_value = pd.DataFrame({"row_count": [3]})
        mock_session.list_table_schema.return_value = {
            "colA": ColumnSpecWithDescription(name="colA", dtype=DBVarType.INT)
        }
        mock_session.generate_session_unique_id = Mock(return_value="1")

        with test_api_client.stream("GET", f"{self.base_route}/pyarrow_table/{doc_id}") as response:
            assert response.status_code == HTTPStatus.OK
            # monkey patch iter_content to iter_bytes to mimick requests behavior
            response.iter_content = response.iter_bytes
            with tempfile.TemporaryDirectory() as temp_dir:
                output_path = Path(os.path.join(temp_dir, "test.parquet"))
                parquet_from_arrow_stream(
                    ResponseStream(response.iter_content(1024)), output_path=output_path, num_rows=3
                )
                downloaded_df = pd.read_parquet(output_path)
        pd.testing.assert_frame_equal(downloaded_df, expected_df)

        assert (
            mock_session.get_async_query_stream.call_args[0][0]
            == textwrap.dedent(
                f"""
                SELECT
                  "colA"
                FROM "sf_database"."sf_schema"."{table_name}"
                """
            ).strip()
        )

    @property
    def download_filename_prefix(self):
        """Prefix for download file name"""
        return self.base_route.lstrip("/")

    def test_download_parquet(
        self, test_api_client_persistent, create_success_response, mock_get_session
    ):
        """Test download parquet (success)"""
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.OK
        result = create_success_response.json()
        table_name = result["name"]
        doc_id = result["_id"]
        expected_df = pd.DataFrame({"colA": [1, 2, 3] * 100})

        async def mock_get_async_query_stream(query):
            _ = query
            buffer = BytesIO()
            batches = pa.Table.from_pandas(expected_df).to_batches(max_chunksize=10)
            writer = create_new_arrow_stream_writer(buffer, batches[0].schema)
            for batch in batches:
                writer.write_batch(batch)
                yield buffer.getvalue()
                buffer.seek(0)
                buffer.truncate(0)

        mock_session = mock_get_session.return_value
        mock_session.get_async_query_stream = Mock(side_effect=mock_get_async_query_stream)
        mock_session.execute_query.return_value = pd.DataFrame({"row_count": [3]})
        mock_session.list_table_schema.return_value = {
            "colA": ColumnSpecWithDescription(name="colA", dtype=DBVarType.INT)
        }
        mock_session.generate_session_unique_id = Mock(return_value="1")

        response = test_api_client.get(f"{self.base_route}/parquet/{doc_id}")
        assert response.status_code == HTTPStatus.OK
        assert len(response.content) > 0
        assert response.headers == {
            "content-disposition": (
                f'attachment; name="data"; filename="{self.download_filename_prefix}_{table_name}.parquet"'
            ),
            "content-type": "application/octet-stream",
        }
        buffer = BytesIO()
        expected_df.to_parquet(buffer, index=False)
        downloaded_df = pd.read_parquet(BytesIO(response.content))
        pd.testing.assert_frame_equal(downloaded_df, expected_df)

    def test_preview(self, test_api_client_persistent, create_success_response, mock_get_session):
        """Test preview (success)"""
        test_api_client, _ = test_api_client_persistent
        assert create_success_response.status_code == HTTPStatus.OK
        result = create_success_response.json()
        doc_id = result["_id"]

        mock_session = mock_get_session.return_value
        mock_session.execute_query.return_value = pd.DataFrame()

        response = test_api_client.post(f"{self.base_route}/{doc_id}/preview")
        assert response.status_code == HTTPStatus.OK
        assert response.json() == {
            "data": (
                '{"schema":{"fields":[{"name":"index","type":"integer"}],"primaryKey":["index"],'
                '"pandas_version":"1.4.0"},"data":[]}'
            ),
            "type_conversions": None,
        }

    @pytest.fixture(autouse=True)
    def auto_patch_snowflake_execute_query(self, snowflake_execute_query_for_materialized_table):
        """Patch SnowflakeSession.execute_query to return mock data"""
        yield snowflake_execute_query_for_materialized_table
