"""
BaseApiTestSuite
"""
# pylint: disable=too-many-lines
import copy
import json
from datetime import datetime
from http import HTTPStatus
from time import sleep

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_CATALOG_ID
from featurebyte.query_graph.node.schema import FeatureStoreDetails
from featurebyte.schema.table import TableCreate


class BaseApiTestSuite:
    """
    BaseApiTestSuite contains common api tests
    """

    # pylint: disable=too-many-public-methods

    # class variables to be set at metaclass
    base_route = None
    class_name = None
    payload = None
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """Setup for post route"""

    @pytest_asyncio.fixture()
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        id_before = self.payload["_id"]
        response = test_api_client.post(f"{self.base_route}", json=self.payload)
        response_dict = response.json()
        assert response.status_code == HTTPStatus.CREATED
        assert response_dict["_id"] == id_before
        return response

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client
        return []

    @pytest_asyncio.fixture()
    async def create_multiple_success_responses(self, test_api_client_persistent):
        """Post multiple success responses"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        output = []
        for _, payload in enumerate(self.multiple_success_payload_generator(test_api_client)):
            # payload name is set here as we need the exact name value for test_list_200 test
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

    @pytest_asyncio.fixture()
    async def create_success_response(self, test_api_client_persistent):
        """Post route success response object"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client)
        id_before = self.payload["_id"]
        response = test_api_client.post(f"{self.base_route}", json=self.payload)

        response = self.wait_for_results(test_api_client, response)
        response_dict = response.json()
        assert response_dict["status"] == "SUCCESS"

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

    @pytest.fixture(name="catalog_id")
    def catalog_id_fixture(self, test_api_client_persistent):
        """
        Create catalog
        """
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.post("/catalog", json={"name": "Test"})
        assert response.status_code == HTTPStatus.CREATED
        return ObjectId(response.json()["_id"])

    @pytest_asyncio.fixture()
    async def create_success_response_non_default_catalog(
        self, test_api_client_persistent, catalog_id
    ):
        """Create object with non default catalog"""
        test_api_client, _ = test_api_client_persistent
        self.setup_creation_route(test_api_client, catalog_id=catalog_id)
        payload = copy.deepcopy(self.payload)
        payload["_id"] = str(ObjectId())
        payload["catalog_id"] = str(catalog_id)
        response = test_api_client.post(
            f"{self.base_route}", params={"catalog_id": catalog_id}, json=payload
        )
        assert response.status_code == HTTPStatus.CREATED, response.json()
        return response

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        # test default catalog id is captured in document
        assert create_success_response.json()["catalog_id"] == str(DEFAULT_CATALOG_ID)

    def test_create_201_non_default_catalog(
        self,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test creation (success) in non default catalog"""
        response = create_success_response_non_default_catalog
        result = response.json()

        # check catalog id is updated correctly
        assert result["catalog_id"] == str(catalog_id)

    def test_list_200_non_default_catalog(
        self,
        test_api_client_persistent,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test list in non default catalog"""
        test_api_client, _ = test_api_client_persistent
        custom_catalog_document_id = create_success_response_non_default_catalog.json()["_id"]

        # expect to see document in the catalog
        response = test_api_client.get(f"{self.base_route}", params={"catalog_id": catalog_id})
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 1
        assert results["data"][0]["_id"] == custom_catalog_document_id

        # expect not to see document in the default catalog
        response = test_api_client.get(f"{self.base_route}")
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["total"] == 0

    def test_get_200_non_default_catalog(
        self,
        test_api_client_persistent,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test get (success)"""
        test_api_client, _ = test_api_client_persistent
        custom_catalog_document_id = create_success_response_non_default_catalog.json()["_id"]

        # expect to see document in the catalog
        response = test_api_client.get(
            f"{self.base_route}/{custom_catalog_document_id}",
            params={"catalog_id": catalog_id},
        )
        assert response.status_code == HTTPStatus.OK
        results = response.json()
        assert results["_id"] == custom_catalog_document_id

        # expect not to see document in the default catalog
        response = test_api_client.get(f"{self.base_route}/{custom_catalog_document_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND


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
                    "__root__\n  "
                    'Column "non-exist-columns" not found in the table! (type=value_error)'
                ),
            ),
            (
                {"record_creation_timestamp_column": "item_id"},
                (
                    f"1 validation error for {self.class_name}Model\n"
                    f"__root__\n  "
                    f"Column \"item_id\" is expected to have type(s): ['TIMESTAMP', 'TIMESTAMP_TZ'] (type=value_error)"
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

    def setup_creation_route(self, api_client, catalog_id=DEFAULT_CATALOG_ID):
        """
        Setup for post route
        """
        api_object_filename_pairs = [
            ("feature_store", "feature_store"),
            ("entity", "entity"),
        ]
        for api_object, filename in api_object_filename_pairs:
            payload = self.load_payload(f"tests/fixtures/request_payloads/{filename}.json")
            response = api_client.post(
                f"/{api_object}", params={"catalog_id": catalog_id}, json=payload
            )
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
        return FeatureStoreDetails(**snowflake_feature_store.dict())

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
            {"name": "event_id", "dtype": "INT", "entity_id": None, "semantic_id": None},
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
        assert response.status_code == HTTPStatus.CREATED
        assert response.json()["_id"] == data_model_dict["_id"]
        return response

    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""
        super().test_create_201(test_api_client_persistent, create_success_response, user_id)
        assert create_success_response.json()["status"] == "DRAFT"

    def test_update_fails_table_not_found(self, test_api_client_persistent, data_update_dict):
        """
        Update Data fails if table not found
        """
        test_api_client, _ = test_api_client_persistent
        random_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{random_id}", json=data_update_dict)
        assert response.status_code == HTTPStatus.NOT_FOUND
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
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json() == {"detail": "Invalid status transition from DRAFT to DEPRECATED."}

    def test_update_status_only(self, test_api_client_persistent, data_response):
        """
        Update Tabular Data status only
        """
        # insert a record
        test_api_client, _ = test_api_client_persistent
        current_data = data_response.json()
        assert current_data.pop("status") == "DRAFT"
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
        assert results["total"] == 3
        assert [record["action_type"] for record in results["data"]] == [
            "UPDATE",
            "UPDATE",
            "INSERT",
        ]
        assert [record["previous_values"].get("status") for record in results["data"]] == [
            "DRAFT",
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
                "loc": ["path", self.id_field_name],
                "msg": "Id must be of type PydanticObjectId",
                "type": "type_error",
            }
        ]

    def test_update_422__entity_id_not_found(
        self, test_api_client_persistent, data_response, columns_info
    ):
        """Test update (unprocessable) - entity ID not found"""
        test_api_client, _ = test_api_client_persistent
        data_response_dict = data_response.json()

        unknown_entity_id = str(ObjectId())
        column = "item_id"
        column_to_update = columns_info[-1]
        assert column_to_update["name"] == column
        column_to_update["entity_id"] = unknown_entity_id
        response = test_api_client.patch(
            f"{self.base_route}/{data_response_dict['_id']}",
            json={"columns_info": columns_info},
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == (
            f"Entity IDs ['{unknown_entity_id}'] not found for columns ['{column}']."
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

    def test_upate_columns_info_422__duplicated_column_name(
        self, test_api_client_persistent, data_response, columns_info
    ):
        """Test update columns unprocessible due to duplicated colum name in columns info"""
        test_api_client, _ = test_api_client_persistent
        response_dict = data_response.json()
        duplicated_col_info = columns_info[0]
        update_response = test_api_client.patch(
            f"{self.base_route}/{response_dict['_id']}",
            json={"columns_info": columns_info + [duplicated_col_info]},
        )
        assert update_response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        duplicated_col_name = duplicated_col_info["name"]
        error_msg = update_response.json()["detail"][0]["msg"]
        assert f'Column name "{duplicated_col_name}" is duplicated.' in error_msg

    def test_update_columns_info(self, test_api_client_persistent, data_response, columns_info):
        """Test update columns info"""
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
        assert update_response.status_code == HTTPStatus.OK

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
        assert response.json()["detail"] == expected_message

    def test_table_get_200(self, test_api_client_persistent, create_success_response):
        """Test table get (success)"""
        test_api_client, _ = test_api_client_persistent
        success_response_dict = create_success_response.json()

        # check that table route can be used to retrieve the created table
        response = test_api_client.get(f"/table/{success_response_dict['_id']}")
        assert response.json() == success_response_dict

    def test_table_list_200(self, test_api_client_persistent, create_multiple_success_responses):
        """Test table list (success, multiple)"""
        # test with default params
        test_api_client, _ = test_api_client_persistent
        _ = create_multiple_success_responses
        response = test_api_client.get("/table")
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
            "/table",
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
            "/table",
            params={"sort_dir": "asc", "sort_by": "name", "page_size": 2, "page": 2},
        )
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_dict = response_with_params.json()
        assert response_with_params_dict.items() >= {**expected_paginated_info, "page": 2}.items()
        response_with_params_names = [elem["name"] for elem in response_with_params_dict["data"]]
        assert response_with_params_names == expected_sorted_names[-1:]

        # test sort_by with some random unknown column name
        # should not throw error, just that the sort_by param has no real effect since column not found
        response_with_params = test_api_client.get("/table", params={"sort_by": "random_name"})
        assert response_with_params.status_code == HTTPStatus.OK

        # test name parameter
        response_with_params = test_api_client.get("/table", params={"name": expected_names[1]})
        assert response_with_params.status_code == HTTPStatus.OK
        response_with_params_names = [elem["name"] for elem in response_with_params.json()["data"]]
        assert response_with_params_names == [expected_names[1]]

        # test bench_size_boundary
        response_page_size_boundary = test_api_client.get("/table", params={"page_size": 100})
        assert response_page_size_boundary.status_code == HTTPStatus.OK
