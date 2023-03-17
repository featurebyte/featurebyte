"""
Test for TaskStatus route
"""
from http import HTTPStatus

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.models.base import DEFAULT_WORKSPACE_ID, User
from featurebyte.service.task_manager import TaskManager
from tests.util.task import LongRunningPayload


class TestTaskStatusApi:
    """Test suite for Task Status API"""

    # class variables to be set at metaclass
    base_route = "/task"

    @pytest.fixture
    def task_manager(self, user_id, persistent):
        """Task manager fixture"""
        yield TaskManager(
            user=User(id=user_id), persistent=persistent, workspace_id=DEFAULT_WORKSPACE_ID
        )

    @pytest_asyncio.fixture
    async def task_status_id(self, user_id, task_manager):
        """Task status id"""
        return await task_manager.submit(
            payload=LongRunningPayload(user_id=user_id, workspace_id=DEFAULT_WORKSPACE_ID)
        )

    def test_get_200(self, test_api_client_persistent, task_status_id, user_id):
        """Test get (success)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}/{task_status_id}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict.items() > {"id": str(task_status_id), "status": "SUCCESS"}.items()
        assert (
            response_dict["payload"].items()
            > {
                "command": "long_running_command",
                "output_collection_name": "long_running_result_collection",
                "user_id": str(user_id),
            }.items()
        )

    def test_get_404(self, test_api_client_persistent):
        """Test get (not found)"""
        test_api_client, _ = test_api_client_persistent
        unknown_id = ObjectId()
        response = test_api_client.get(f"{self.base_route}/{unknown_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json()["detail"] == f'Task (id: "{unknown_id}") not found.'

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sort_dir", ["desc", "asc"])
    async def test_list_200(self, user_id, task_manager, test_api_client_persistent, sort_dir):
        """Test list (success, multiple)"""
        expected_task_ids = []
        for _ in range(3):
            task_id = await task_manager.submit(
                payload=LongRunningPayload(user_id=user_id, workspace_id=DEFAULT_WORKSPACE_ID)
            )
            expected_task_ids.append(task_id)
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(self.base_route, params={"sort_dir": sort_dir})
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["total"] == len(response_dict["data"])
        assert [data["id"] for data in response_dict["data"]] == (
            expected_task_ids if sort_dir == "asc" else list(reversed(expected_task_ids))
        )
