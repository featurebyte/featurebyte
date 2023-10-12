"""
Test task
"""
from __future__ import annotations

from typing import Any

from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.test import TestTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class TestTask(BaseTask[TestTaskPayload]):
    """
    Test Task
    """

    payload_class = TestTaskPayload

    def __init__(
        self,
        task_id: UUID,
        progress: Any,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_id=task_id, progress=progress)
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: TestTaskPayload) -> str:
        return "Run Test task"

    async def execute(self, payload: TestTaskPayload) -> Any:
        logger.debug("Test task started")
        for percent in range(0, 100, 20):
            await self.task_progress_updater.update_progress(percent=percent)
        logger.debug("Test task completed")
