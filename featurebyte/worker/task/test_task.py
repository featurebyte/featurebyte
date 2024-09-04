"""
Test task
"""

from __future__ import annotations

import asyncio
from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.task import TaskStatus
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
        super().__init__(task_manager=task_manager)
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: TestTaskPayload) -> str:
        return "Run Test task"

    async def execute(self, payload: TestTaskPayload) -> Any:
        logger.debug("Test task started")
        if payload.run_child_task:
            child_task_id = await self.submit_child_task(
                payload=self.payload_class(
                    user_id=payload.user_id,
                    catalog_id=payload.catalog_id,
                    run_child_task=False,
                ),
            )

            # wait for child task to complete
            while True:
                child_task = await self.task_manager.get_task(task_id=str(child_task_id))
                if child_task and child_task.status in TaskStatus.terminal():
                    break

                # sleep for a second
                await asyncio.sleep(1)
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
