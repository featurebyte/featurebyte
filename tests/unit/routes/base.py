"""
BaseApiTestSuite
"""
import json
from datetime import datetime
from http import HTTPStatus
from time import sleep

import pytest
from bson.objectid import ObjectId


class BaseApiTestSuite:
    """
    BaseApiTestSuite contains common api tests
    """

    # pylint: disable=too-many-public-methods

    # class variables to be set at metaclass
    base_route = None
    class_name = None
    payload_filename = None
    payload = None
    has_update_method = True
    create_conflict_payload_expected_detail_pairs = []
    create_unprocessable_payload_expected_detail_pairs = []
    list_unprocessable_params_expected_detail_pairs = [
        (
            {"page_size": 0},
            [
                {
                    "loc": ["query", "page_size"],
                    "msg": "ensure this value is greater than 0",
                    "type": "value_error.number.not_gt",
                    "ctx": {"limit_value": 0},
                },
            ],
        ),
        (
            {"page_size": 101},
            [
                {
                    "loc": ["query", "page_size"],
                    "msg": "ensure this value is less than or equal to 100",
                    "type": "value_error.number.not_le",
                    "ctx": {"limit_value": 100},
                },
            ],
        ),
        (
            {"page_size": "abcd"},
            [
                {
                    "loc": ["query", "page_size"],
                    "msg": "value is not a valid integer",
                    "type": "type_error.integer",
                },
            ],
        ),
        (
            {"sort_by": "", "search": ""},
            [
                {
                    "loc": ["query", "sort_by"],
                    "msg": "ensure this value has at least 1 characters",
                    "type": "value_error.any_str.min_length",
                    "ctx": {"limit_value": 1},
                },
                {
                    "loc": ["query", "search"],
                    "msg": "ensure this value has at least 1 characters",
                    "type": "value_error.any_str.min_length",
                    "ctx": {"limit_value": 1},
                },
            ],
        ),
        (
            {"sort_dir": "abcd"},
            [
                {
                    "loc": ["query", "sort_dir"],
                    "msg": 'string does not match regex "^(asc|desc)$"',
                    "type": "value_error.str.regex",
                    "ctx": {"pattern": "^(asc|desc)$"},
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
        return self.base_route.lstrip("/") + "_id"

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

    def setup_creation_route(self, api_client):
        """Setup for post route"""

    @pytest.fixture()
    def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        id_before = self.payload["_id"]
        response = test_api_client.post(f"{self.base_route}", json=self.payload)
        response_dict = response.json()
        assert response_dict["_id"] == id_before
        return response

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        return []

    @pytest.fixture()
    def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        output = []
        for i, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
            payload["name"] = f'{self.payload["name"]}_{i}'
            response = test_api_client.post(f"{self.base_route}", json=payload)
            assert response.status_code == HTTPStatus.CREATED
            output.append(response)
        return output

    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        """Test creation (success) without specifying id field"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = {key: value for key, value in self.payload.items() if key != "_id"}
        assert "_id" not in payload
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.CREATED

    def test_create_201__id_is_none(self, test_api_client_persistent):
        """Test creation (success) ID is None"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        payload = self.payload.copy()
        payload["_id"] = None
        response = test_api_client.post(f"{self.base_route}", json=payload)
        assert response.status_code == HTTPStatus.CREATED

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        assert create_success_response.status_code == HTTPStatus.CREATED
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
        response = test_api_client.post(f"{self.base_route}", json=conflict_payload)
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
        response = test_api_client.post(f"{self.base_route}", json=unprocessable_payload)
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
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
        assert response_dict["updated_at"] is None
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
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "loc": ["path", self.id_field_name],
                "msg": "Id must be of type PydanticObjectId",
                "type": "type_error",
            }
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
        payload_name = self.payload["name"]

        assert len(response_dict["data"]) == 3
        assert response_dict.items() >= expected_paginated_info.items()
        response_data_names = [elem["name"] for elem in response_dict["data"]]
        assert response_data_names == [f"{payload_name}_{i}" for i in reversed(range(3))]

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
        assert response_with_params_names == [f"{payload_name}_0", f"{payload_name}_1"]

        # test with pagination parameters (page 2)
        response_with_params = test_api_client.get(
            f"{self.base_route}",
            params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 2},
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_dict = response_with_params.json()
        assert response_with_params_dict.items() >= {**expected_paginated_info, "page": 2}.items()
        response_with_params_names = [elem["name"] for elem in response_with_params_dict["data"]]
        assert response_with_params_names == [f"{payload_name}_2"]

        # test sort_by with some random unknown column name
        # should not throw error, just that the sort_by param has no real effect since column not found
        response_with_params = test_api_client.get(
            f"{self.base_route}", params={"sort_by": "random_name"}
        )
        assert response_with_params.status_code == HTTPStatus.OK

        # test name parameter
        response_with_params = test_api_client.get(
            f"{self.base_route}", params={"name": f"{payload_name}_1"}
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_names = [elem["name"] for elem in response_with_params.json()["data"]]
        assert response_with_params_names == [f"{payload_name}_1"]

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
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == [
            {
                "loc": ["path", self.id_field_name],
                "msg": "Id must be of type PydanticObjectId",
                "type": "type_error",
            }
        ]


class BaseAsyncApiTestSuite(BaseApiTestSuite):
    """
    BaseAsyncApiTestSuite contains common api tests with async creation routes
    """

    time_limit = 10

    def wait_for_results(self, api_client, create_response):
        """
        Wait for async job to complete
        """
        task_submission = create_response.json()
        task_id = task_submission["id"]
        output_path = task_submission["output_path"]

        start_time = datetime.now()
        while (datetime.now() - start_time).seconds < self.time_limit:
            response = api_client.get(f"/task/{task_id}")
            task_status = response.json()
            status = task_status["status"]
            if status not in ["PENDING", "RECEIVED", "STARTED"]:
                assert status == "SUCCESS"
                break
            sleep(0.1)

        return output_path

    @pytest.fixture()
    def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        id_before = self.payload["_id"]
        response = test_api_client.post(f"{self.base_route}", json=self.payload)

        output_path = self.wait_for_results(test_api_client, response)

        response = test_api_client.get(output_path)
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
