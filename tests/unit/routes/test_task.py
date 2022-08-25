"""
Test for TaskStatus route
"""
from http import HTTPStatus
from unittest.mock import patch

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.process_store import ProcessStore
from tests.util.task import Command, LongRunningPayload, TaskExecutor

ProcessStore._command_class = Command
ProcessStore._task_executor = TaskExecutor


class TestTaskStatusApi:

    # class variables to be set at metaclass
    base_route = "/task"

    @pytest.fixture
    def task_manager(self, user_id):
        """Task manager fixture"""
        with patch("featurebyte.service.task_manager.ProcessStore", wraps=ProcessStore):
            return TaskManager(user_id=user_id)

    @pytest.fixture(autouse=True)
    def patch_controller_task_manager(self, task_manager):
        """Patch task manager in task status controller"""
        with patch("featurebyte.app.TaskManager") as mock_task_manager:
            mock_task_manager.return_value = task_manager
            yield

    @pytest_asyncio.fixture
    async def task_status_id(self, user_id, task_manager):
        """Task status id"""
        return await task_manager.submit(payload=LongRunningPayload(user_id=user_id))

    @pytest.mark.no_mock_process_store
    def test_get_200(self, test_api_client_persistent, task_status_id, user_id):
        """Test get (success)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(f"{self.base_route}/{task_status_id}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict.items() > {"id": str(task_status_id), "status": "STARTED"}.items()
        assert (
            response_dict["payload"].items()
            > {
                "command": "long_running_command",
                "output_collection_name": "long_running_result_collection",
                "user_id": str(user_id),
            }.items()
        )

    @pytest.mark.no_mock_process_store
    def test_get_404(self, test_api_client_persistent):
        """Test get (not found)"""
        test_api_client, _ = test_api_client_persistent
        unknown_id = ObjectId()
        response = test_api_client.get(f"{self.base_route}/{unknown_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json()["detail"] == f'Task (id: "{unknown_id}") not found.'

    @pytest.mark.parametrize("sort_dir, reverse", [("desc", True), ("asc", False)])
    def test_list_200(self, test_api_client_persistent, sort_dir, reverse):
        """Test list (success, multiple)"""
        test_api_client, _ = test_api_client_persistent
        response = test_api_client.get(self.base_route, params={"sort_dir": sort_dir})
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["total"] == len(response_dict["data"])
        assert response_dict["data"] == sorted(
            response_dict["data"], key=lambda d: d["id"], reverse=reverse
        )
