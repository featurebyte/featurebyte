"""
Tests functions/methods in api_object.py
"""
from http import HTTPStatus
from unittest.mock import Mock, patch

import pytest

from featurebyte.api.api_object import ApiGetObject, ApiObject
from featurebyte.exception import RecordCreationException, RecordRetrievalException
from featurebyte.schema.task_status import TaskStatus


@pytest.fixture(name="mock_configuration")
def mock_configuration_fixture(request):
    """Mock configuration (page_size is parametrized)"""

    def fake_get(url, params):
        _ = url
        page = params["page"]
        page_size, total = request.param, 11
        data = [
            {"name": f"item_{i + (page - 1) * page_size}"}
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
    output = ApiGetObject.list()
    assert output == [f"item_{i}" for i in range(11)]


@pytest.fixture(name="mock_client")
def mock_client_fixture():
    """Mock client fixture"""

    class FakeResponse:
        """FakeResponse class"""

        def __init__(self, status_code, response_dict):
            self.status_code = status_code
            self.response_dict = response_dict

        def json(self):
            return self.response_dict

    def post_side_effect(url, json):
        """Post side effect"""
        _ = json
        return {
            "success_task_started": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={
                    "status": TaskStatus.STARTED,
                    "output_path": "get_result_success",
                    "id": "success_id",
                },
            ),
            "success_task_success": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={
                    "status": TaskStatus.SUCCESS,
                    "output_path": "get_result_success",
                    "id": "success_id",
                },
            ),
            "post_failure": FakeResponse(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, response_dict={}
            ),
            "post_success_task_started": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={"status": TaskStatus.STARTED, "id": "failure_id"},
            ),
            "post_success_task_failure": FakeResponse(
                status_code=HTTPStatus.CREATED, response_dict={"status": TaskStatus.FAILURE}
            ),
            "post_success_get_task_failure": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={"status": TaskStatus.STARTED, "id": "get_failure_id"},
            ),
            "post_success_get_result_failure": FakeResponse(
                status_code=HTTPStatus.CREATED,
                response_dict={"status": TaskStatus.SUCCESS, "output_path": "get_result_failure"},
            ),
        }[url]

    def get_side_effect(url):
        """Get side effect"""
        return {
            "/task/success_id": FakeResponse(
                status_code=HTTPStatus.OK, response_dict={"status": TaskStatus.SUCCESS}
            ),
            "/task/failure_id": FakeResponse(
                status_code=HTTPStatus.OK, response_dict={"status": TaskStatus.FAILURE}
            ),
            "/task/get_failure_id": FakeResponse(
                status_code=HTTPStatus.NOT_FOUND, response_dict={}
            ),
            "get_result_success": FakeResponse(
                status_code=HTTPStatus.OK, response_dict={"result": "some_value"}
            ),
            "get_result_failure": FakeResponse(status_code=HTTPStatus.NOT_FOUND, response_dict={}),
        }[url]

    with patch("featurebyte.api.api_object.Configurations") as mock_config:
        mock_client = mock_config.return_value.get_client.return_value
        mock_client.post.side_effect = post_side_effect
        mock_client.get.side_effect = get_side_effect
        yield mock_client


@pytest.mark.parametrize("route", ["success_task_started", "success_task_success"])
def test_post_async_task__success(mock_client, route):
    """Test post async task (success)"""
    output = ApiObject.post_async_task(route=route, payload={})
    assert output == {"result": "some_value"}


@pytest.mark.parametrize(
    "route", ["post_failure", "post_success_task_started", "post_success_task_failure"]
)
def test_post_async_task__record_creation_exception(mock_client, route):
    """Test post async task (success)"""
    with pytest.raises(RecordCreationException):
        ApiObject.post_async_task(route=route, payload={})


@pytest.mark.parametrize(
    "route", ["post_success_get_task_failure", "post_success_get_result_failure"]
)
def test_post_async_task__record_retrieval_exception(mock_client, route):
    """Test post async task (success)"""
    with pytest.raises(RecordRetrievalException):
        ApiObject.post_async_task(route=route, payload={})
