"""
Test task
"""

from __future__ import annotations

import asyncio
from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.test import TestIOTaskPayload, TestTaskPayload
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class TestTask(BaseTask[TestTaskPayload]):
    """
    Test CPU Task
    """

    payload_class = TestTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__()
        self.task_manager = task_manager
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: TestTaskPayload) -> str:
        return "Run Test task"

    async def execute(self, payload: TestTaskPayload) -> Any:
        logger.debug("Test task started")

        if payload.run_child_task:
            await self.task_manager.submit(
                payload=self.payload_class(
                    user_id=payload.user_id,
                    catalog_id=payload.catalog_id,
                    run_child_task=False,
                ),
                parent_task_id=str(payload.task_id),
            )
        else:
            for percent in range(0, 100, 20):
                await self.task_progress_updater.update_progress(percent=percent)
                if payload.sleep:
                    await asyncio.sleep(payload.sleep)
        logger.debug("Test task completed")
        return "Test task result"


class TestIOTask(TestTask):
    """
    Test IO Task
    """

    payload_class = TestIOTaskPayload
