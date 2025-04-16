"""
Test for TaskStatus route
"""

from http import HTTPStatus

import pytest
import pytest_asyncio
from bson.objectid import ObjectId

from featurebyte.common import DEFAULT_CATALOG_ID
from featurebyte.schema.worker.task.test import TestIOTaskPayload
from featurebyte.worker.test_util.random_task import LongRunningPayload


class TestTaskApi:
    """Test suite for Task API"""

    # class variables to be set at metaclass
    base_route = "/task"

    @pytest.fixture
    def task_manager(self, app_container):
        """Task manager fixture"""
        yield app_container.task_manager

    @pytest_asyncio.fixture
    async def task_id(self, user_id, task_manager):
        """Task id"""
        return await task_manager.submit(
            payload=LongRunningPayload(user_id=user_id, catalog_id=DEFAULT_CATALOG_ID)
        )

    def test_get_200(self, api_client_persistent, task_id, user_id):
        """Test get (success)"""
        test_api_client, _ = api_client_persistent
        response = test_api_client.get(f"{self.base_route}/{task_id}")
        assert response.status_code == HTTPStatus.OK
        response_dict = response.json()
        assert response_dict.items() > {"id": str(task_id), "status": "SUCCESS"}.items()
        assert (
            response_dict["payload"].items()
            > {
                "command": "long_running_command",
                "output_collection_name": "long_running_result_collection",
                "user_id": str(user_id),
            }.items()
        )

    def test_get_404(self, api_client_persistent):
        """Test get (not found)"""
        test_api_client, _ = api_client_persistent
        unknown_id = ObjectId()
        response = test_api_client.get(f"{self.base_route}/{unknown_id}")
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json()["detail"] == f'Task (id: "{unknown_id}") not found.'

    @pytest.mark.asyncio
    @pytest.mark.parametrize("sort_dir", ["desc", "asc"])
    async def test_list_200(self, user_id, task_manager, api_client_persistent, sort_dir):
        """Test list (success, multiple)"""
        expected_task_ids = []
        for _ in range(3):
            task_id = await task_manager.submit(
                payload=LongRunningPayload(user_id=user_id, catalog_id=DEFAULT_CATALOG_ID)
            )
            expected_task_ids.append(task_id)
        test_api_client, _ = api_client_persistent
        response = test_api_client.get(self.base_route, params={"sort_dir": sort_dir})
        response_dict = response.json()
        assert response.status_code == HTTPStatus.OK
        assert response_dict["total"] == len(response_dict["data"])
        assert [data["id"] for data in response_dict["data"]] == (
            expected_task_ids if sort_dir == "asc" else list(reversed(expected_task_ids))
        )

    def test_patch_404(self, api_client_persistent):
        """Test patch (not found)"""
        test_api_client, _ = api_client_persistent
        unknown_id = ObjectId()
        response = test_api_client.patch(f"{self.base_route}/{unknown_id}", json={"revoke": True})
        assert response.status_code == HTTPStatus.NOT_FOUND
        assert response.json()["detail"] == f'Task (id: "{unknown_id}") not found.'

    @pytest.mark.asyncio
    async def test_patch_422(self, api_client_persistent, task_manager, user_id):
        """Test patch (not revocable)"""
        test_api_client, _ = api_client_persistent
        task_id = await task_manager.submit(
            payload=TestIOTaskPayload(user_id=user_id, catalog_id=DEFAULT_CATALOG_ID)
        )
        response = test_api_client.patch(f"{self.base_route}/{task_id}", json={"revoke": True})
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        assert response.json()["detail"] == f'Task (id: "{task_id}") does not support revoke.'

    def test_patch_200(self, api_client_persistent, task_id):
        """Test patch"""
        test_api_client, _ = api_client_persistent
        response = test_api_client.patch(f"{self.base_route}/{task_id}", json={"revoke": True})
        assert response.status_code == HTTPStatus.OK

    @pytest.mark.asyncio
    async def test_post_422__task_not_rerunnable(
        self, api_client_persistent, task_manager, user_id
    ):
        """Test post (not rerunnable)"""
        test_api_client, _ = api_client_persistent
        task_id = await task_manager.submit(
            payload=TestIOTaskPayload(user_id=user_id, catalog_id=DEFAULT_CATALOG_ID)
        )
        response = test_api_client.post(f"{self.base_route}/{task_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == f'Task (id: "{task_id}") does not support rerun.'

    def test_post_422__task_not_unsuccessful(self, api_client_persistent, task_id):
        """Test post (task is not unsuccessful)"""
        test_api_client, _ = api_client_persistent
        response = test_api_client.post(f"{self.base_route}/{task_id}")
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY, response.json()
        assert response.json()["detail"] == f'Task (id: "{task_id}") does not support rerun.'

    @pytest.mark.asyncio
    async def test_post_200(self, api_client_persistent, task_id):
        """Test post"""
        test_api_client, persistent = api_client_persistent
        await persistent.update_one(
            collection_name="celery_taskmeta",
            query_filter={"_id": task_id},
            update={"$set": {"status": "FAILURE"}},
            user_id=None,
        )

        response = test_api_client.post(f"{self.base_route}/{task_id}")
        assert response.status_code == HTTPStatus.CREATED, response.json()
