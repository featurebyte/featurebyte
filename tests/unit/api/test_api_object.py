"""
Tests functions/methods in api_object.py
"""
import time
from http import HTTPStatus
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from bson import ObjectId
from pandas.testing import assert_frame_equal

from featurebyte.api.api_object import ApiObject
from featurebyte.api.feature import Feature, FeatureNamespace
from featurebyte.api.feature_list import FeatureListNamespace
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.models.feature import (
    FeatureModel,
    FeatureNamespaceModel,
    FrozenFeatureNamespaceModel,
)
from featurebyte.models.feature_list import (
    FeatureListNamespaceModel,
    FrozenFeatureListNamespaceModel,
)
from featurebyte.schema.feature import FeatureUpdate
from featurebyte.schema.feature_list_namespace import FeatureListNamespaceUpdate
from featurebyte.schema.feature_namespace import FeatureNamespaceUpdate
from featurebyte.schema.task import TaskStatus


@pytest.fixture(name="mock_configuration")
def mock_configuration_fixture():
    """Mock configuration (page_size is parametrized)"""

    def fake_get(url, params):
        _ = url
        page = params.get("page", 1)
        page_size, total = params.get("page_size", 10), 11
        data = [
            {
                "_id": f"637b87ee8959fd0e36a0bc{i + (page - 1) * page_size:02d}",
                "name": f"item_{i + (page - 1) * page_size}",
                "created_at": "2022-11-21T14:00:49.255000",
            }
            for i in range(page_size)
            if (i + (page - 1) * page_size) < total
        ]
        response_dict = {"page": page, "page_size": page_size, "total": total, "data": data}
        response = Mock()
        response.json.return_value = response_dict
        response.status_code = HTTPStatus.OK
        return response

    with patch("featurebyte.api.api_object.Configurations") as mock_config:
        mock_client = mock_config.return_value.get_client.return_value
        mock_client.get = fake_get
        yield mock_config


@pytest.mark.parametrize("mock_configuration", [1, 3, 5, 11, 25], indirect=True)
def test_list(mock_configuration):
    """Test pagination list logic"""
    output = ApiObject.list()
    assert_frame_equal(
        output,
        pd.DataFrame(
            {
                "id": [ObjectId(f"637b87ee8959fd0e36a0bc{i:02d}") for i in range(11)],
                "name": [f"item_{i}" for i in range(11)],
                "created_at": pd.to_datetime(["2022-11-21T14:00:49.255000"] * 11),
            }
        ),
    )


@pytest.fixture(name="mock_clients")
def mock_clients_fixture():
    """Mock clients fixture"""

    class FakeResponse:
        """FakeResponse class"""

        def __init__(self, status_code, response_dict):
            self.status_code = status_code
            self.response_dict = response_dict
            self.text = ""

        def json(self):
            return self.response_dict

    def post_side_effect(url, **kwargs):
        """Post side effect"""
        _ = kwargs
        return {
            "/success_task_pending": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={
                    "status": TaskStatus.PENDING,
                    "output_path": None,
                    "id": "success_id",
                },
            ),
            "/success_task_started": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={
                    "status": TaskStatus.STARTED,
                    "output_path": None,
                    "id": "success_id",
                },
            ),
            "/success_task_success": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={
                    "status": TaskStatus.SUCCESS,
                    "output_path": "/get_result_success",
                    "id": "success_id",
                },
            ),
            "/post_failure": FakeResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, response_dict={}
            ),
            "/post_success_task_started": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={"status": TaskStatus.STARTED, "id": "failure_id"},
            ),
            "/post_success_task_failure": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={"status": TaskStatus.FAILURE, "id": "failure_id"},
            ),
            "/post_success_get_task_failure": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={"status": TaskStatus.STARTED, "id": "get_failure_id"},
            ),
            "/post_success_get_result_failure": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={
                    "status": TaskStatus.SUCCESS,
                    "id": "get_failure_id",
                    "output_path": "/get_result_failure",
                },
            ),
        }[url]

    def get_side_effect(url):
        """Get side effect"""
        return {
            "/task/success_id": FakeResponse(
                status_code=HTTPStatus.OK,
                response_dict={
                    "status": TaskStatus.SUCCESS,
                    "output_path": "/get_result_success",
                    "id": "success_id",
                },
            ),
            "/task/failure_id": FakeResponse(
                status_code=HTTPStatus.OK, response_dict={"status": TaskStatus.FAILURE}
            ),
            "/task/get_failure_id": FakeResponse(
                status_code=HTTPStatus.NOT_FOUND, response_dict={}
            ),
            "/get_result_success": FakeResponse(
                status_code=HTTPStatus.OK, response_dict={"result": "some_value"}
            ),
            "/get_result_failure": FakeResponse(status_code=HTTPStatus.NOT_FOUND, response_dict={}),
        }[url]

    with patch("featurebyte.api.api_object.Configurations") as mock_config:
        mock_client = mock_config.return_value.get_client.return_value
        mock_client.post.side_effect = post_side_effect
        mock_client.get.side_effect = get_side_effect
        mock_websocket_client = mock_config.return_value.get_websocket_client.return_value
        mock_ws_client = mock_websocket_client.__enter__.return_value
        mock_ws_client.receive_json.return_value = None
        yield mock_client, mock_ws_client


@pytest.mark.parametrize(
    "route", ["/success_task_pending", "/success_task_started", "/success_task_success"]
)
def test_post_async_task__success(mock_clients, route):
    """Test post async task (success)"""
    output = ApiObject.post_async_task(route=route, payload={})
    assert output == {"result": "some_value"}


@pytest.mark.parametrize(
    "route", ["/post_failure", "/post_success_task_started", "/post_success_task_failure"]
)
def test_post_async_task__record_creation_exception(mock_clients, route):
    """Test post async task (success)"""
    with pytest.raises(RecordCreationException):
        ApiObject.post_async_task(route=route, payload={})


@pytest.mark.parametrize(
    "route", ["/post_success_get_task_failure", "/post_success_get_result_failure"]
)
def test_post_async_task__record_retrieval_exception(mock_clients, route):
    """Test post async task (success)"""

    def blocking_receive_json():
        """Blocking receive json to test post async task not blocking"""
        while True:
            time.sleep(1)

    _, mock_ws_client = mock_clients
    mock_ws_client.receive_json.side_effect = blocking_receive_json
    with pytest.raises(RecordRetrievalException):
        ApiObject.post_async_task(route=route, payload={})


def test_api_object_repr(mock_configuration):
    """Test ApiObject repr returns representation from info()"""
    with patch.object(ApiObject, "info") as mock_info:
        mock_info.return_value = "mock_info_result"
        item = ApiObject.get("item_1")
        assert (
            repr(item)
            == f"<featurebyte.api.api_object.ApiObject at {hex(id(item))}>\n'mock_info_result'"
        )


def test_api_object_list_empty():
    """Test ApiObject list returns None if list is empty"""
    with patch("featurebyte.api.api_object.Configurations") as mock_config:
        mock_client = mock_config.return_value.get_client.return_value
        response_dict = {"page": 1, "page_size": 10, "total": 0, "data": []}
        response = Mock()
        response.json.return_value = response_dict
        response.status_code = HTTPStatus.OK
        mock_client.get.return_value = response
        assert_frame_equal(ApiObject.list(), pd.DataFrame(columns=["id", "name", "created_at"]))


@pytest.mark.parametrize(
    "api_object_class,frozen_model_class,model_class,update_schema_class",
    [
        (
            FeatureNamespace,
            FrozenFeatureNamespaceModel,
            FeatureNamespaceModel,
            FeatureNamespaceUpdate,
        ),
        (
            FeatureListNamespace,
            FrozenFeatureListNamespaceModel,
            FeatureListNamespaceModel,
            FeatureListNamespaceUpdate,
        ),
    ],
)
def test_api_object_and_model_attribute_consistencies(
    api_object_class, frozen_model_class, model_class, update_schema_class
):
    """
    This test is used to check that those updatable attributes from the model has the corresponding property at the
    api object
    """
    frozen_attributes = set(frozen_model_class.__fields__)
    dynamic_attributes = set(model_class.__fields__).difference(frozen_attributes)

    # attribute specific to the model (but not frozen model) should become a property in api object class
    for attr in dynamic_attributes:
        assert isinstance(getattr(api_object_class, attr), property)

    # attributes that can be updated through api should not appear in the attributes of the frozen model
    updatable_attributes = set(update_schema_class.__fields__)
    assert updatable_attributes.intersection(frozen_attributes) == set()
