"""
Tests for PeriodicTask route
"""
import json
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson.objectid import ObjectId
from requests import Response

from featurebyte.models.base import DEFAULT_CATALOG_ID, User
from featurebyte.models.periodic_task import Interval, PeriodicTask
from featurebyte.service.periodic_task import PeriodicTaskService
from tests.unit.routes.base import BaseApiTestSuite


class MockResponse(Response):
    """
    Mock response object
    """

    def __init__(self, content: str, status_code: int):
        super().__init__()
        self.encoding = "utf-8"
        self._content = content.encode(self.encoding)
        self.status_code = status_code


class TestPeriodicTaskApi(BaseApiTestSuite):
    """
    Tests for PeriodicTask route
    """

    class_name = "PeriodicTask"
    base_route = "/periodic_task"
    payload = PeriodicTask(
        name="some task",
        task="featurebyte.worker.task_executor.execute_task",
        interval=Interval(every=1, period="minutes"),
        args=[],
        kwargs={"some_key": "some_value"},
        catalog_id=DEFAULT_CATALOG_ID,
    ).dict()
    unknown_id = ObjectId()

    @pytest.mark.skip("POST method not exposed")
    def test_create_201(self, test_api_client_persistent, create_success_response, user_id):
        """Test creation (success)"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201__without_specifying_id_field(self, test_api_client_persistent):
        """Test creation (success) without specifying id field"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201__id_is_none(self, test_api_client_persistent):
        """Test creation (success) ID is None"""

    @pytest.mark.skip("POST method not exposed")
    def test_create_201_non_default_catalog(
        self,
        catalog_id,
        create_success_response_non_default_catalog,
    ):
        """Test creation (success) in non default catalog"""

    @pytest.mark.skip("GET method not exposed")
    def test_list_audit_422(
        self,
        test_api_client_persistent,
        create_multiple_success_responses,
        list_unprocessable_params_expected_detail,
    ):
        """Test list audit (unprocessable)"""

    @pytest.mark.skip("GET method not exposed")
    def test_list_audit_422__invalid_id_value(self, test_api_client_persistent):
        """Test list audit (unprocessable) - invalid id value"""

    @pytest_asyncio.fixture()
    async def create_success_response(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post route success response object"""
        _, persistent = test_api_client_persistent
        periodic_task_service = PeriodicTaskService(
            user=User(id=user_id), persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
        document = await periodic_task_service.create_document(data=PeriodicTask(**self.payload))
        return MockResponse(
            content=json.dumps(document.json_dict()), status_code=HTTPStatus.CREATED
        )

    @pytest_asyncio.fixture()
    async def create_multiple_success_responses(
        self, test_api_client_persistent, user_id
    ):  # pylint: disable=arguments-differ
        """Post multiple success responses"""
        _, persistent = test_api_client_persistent
        output = []
        periodic_task_service = PeriodicTaskService(
            user=User(id=user_id), persistent=persistent, catalog_id=DEFAULT_CATALOG_ID
        )
        for payload in self.multiple_success_payload_generator(None):
            document = await periodic_task_service.create_document(data=PeriodicTask(**payload))
            output.append(
                MockResponse(
                    content=json.dumps(document.json_dict()), status_code=HTTPStatus.CREATED
                )
            )
        return output

    def multiple_success_payload_generator(self, api_client):
        """Create multiple payload for setting up create_multiple_success_responses fixture"""
        _ = api_client

        # default catalog
        payload = self.payload.copy()
        yield payload

        for i in range(2):
            data = self.payload.copy()
            data["_id"] = ObjectId()
            data["name"] = f'{self.payload["name"]}_{i}'
            yield PeriodicTask(**data).json_dict()
