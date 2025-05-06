"""
Base class for testing worker tasks
"""

from __future__ import annotations

import json
from abc import abstractmethod
from typing import Any, Dict, Optional
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from featurebyte.models.base import User
from featurebyte.routes.lazy_app_container import LazyAppContainer
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


class BaseTaskTestSuite:
    """
    Test suite for worker tasks
    """

    # class variables to be set at metaclass
    task_class: BaseTask = None
    payload: Optional[Dict[str, Any]] = None

    @staticmethod
    def load_payload(filename):
        """Load payload"""
        with open(filename) as fhandle:
            return json.loads(fhandle.read())

    @abstractmethod
    async def setup_persistent_storage(self, persistent, storage, temp_storage, catalog):
        """
        Setup records in persistent and storage for testing
        """

    @pytest.fixture(autouse=True)
    def mock_env(self, mock_config_path_env):
        """
        Apply patch on config path env
        """
        _ = mock_config_path_env
        yield

    @pytest.fixture(autouse=True)
    def mock_snowflake(self, snowflake_connector, snowflake_execute_query):
        """
        Apply patch on snowflake operations
        """
        _ = snowflake_connector
        _ = snowflake_execute_query
        yield

    @pytest_asyncio.fixture(autouse=True)
    async def setup(self, mongo_persistent, storage, temp_storage, catalog):
        """
        Run setup
        """
        persistent, _ = mongo_persistent
        await self.setup_persistent_storage(persistent, storage, temp_storage, catalog)

    @pytest.fixture(name="progress")
    def mock_progress(self):
        """
        Mock progress
        """
        yield AsyncMock()

    async def execute_task(
        self,
        user_id,
        task_class,
        payload,
        persistent,
        progress,
        storage,
        temp_storage,
        app_container: LazyAppContainer,
    ):
        """
        Execute task with payload
        """

        user = User(id=user_id)
        app_container.override_instance_for_test("persistent", persistent)
        app_container.override_instance_for_test("user", user)
        app_container.override_instance_for_test("temp_storage", temp_storage)
        app_container.override_instance_for_test("storage", storage)
        app_container.override_instance_for_test("progress", progress)
        task = app_container.get(task_class)
        task_payload = task.get_payload_obj(payload)
        await task.execute(task_payload)

    @pytest_asyncio.fixture()
    async def task_completed(
        self, mongo_persistent, progress, storage, temp_storage, catalog, app_container, user_id
    ):
        """
        Test execution of the task
        """
        persistent, _ = mongo_persistent
        self.payload["catalog_id"] = catalog.id
        app_container.invalidate_dep_for_test(TaskProgressUpdater)
        await self.execute_task(
            user_id=user_id,
            task_class=self.task_class,
            payload=self.payload,
            persistent=persistent,
            progress=progress,
            storage=storage,
            temp_storage=temp_storage,
            app_container=app_container,
        )
