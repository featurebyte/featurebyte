"""
Base class for testing worker tasks
"""
from __future__ import annotations

from typing import Any, Dict, Optional

import json
from abc import abstractmethod
from unittest.mock import Mock

import pytest
import pytest_asyncio

from featurebyte.app import get_celery
from featurebyte.models.base import User
from featurebyte.worker.task.base import BaseTask


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
    async def setup_persistent_storage(self, persistent, storage, temp_storage):
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
    async def setup(self, mongo_persistent, storage, temp_storage):
        """
        Run setup
        """
        persistent, _ = mongo_persistent
        await self.setup_persistent_storage(persistent, storage, temp_storage)

    @pytest.fixture(name="progress")
    def mock_progress(self):
        """
        Mock progress
        """
        yield Mock()

    async def execute_task(
        self, task_class, payload, persistent, progress, storage, temp_storage, get_credential
    ):
        """
        Execute task with payload
        """
        # pylint: disable=not-callable
        task = task_class(
            payload,
            progress=progress,
            user=User(id=payload.get("user_id")),
            get_persistent=lambda: persistent,
            get_credential=get_credential,
            get_storage=lambda: storage,
            get_temp_storage=lambda: temp_storage,
            get_celery=get_celery,
        )

        await task.execute()

    @pytest_asyncio.fixture()
    async def task_completed(
        self, mongo_persistent, progress, storage, temp_storage, get_credential
    ):
        """
        Test execution of the task
        """
        persistent, _ = mongo_persistent
        await self.execute_task(
            task_class=self.task_class,
            payload=self.payload,
            persistent=persistent,
            progress=progress,
            storage=storage,
            temp_storage=temp_storage,
            get_credential=get_credential,
        )
