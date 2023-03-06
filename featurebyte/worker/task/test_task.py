"""
Test task
"""
from __future__ import annotations

from typing import Any

from featurebyte.logger import logger
from featurebyte.schema.worker.task.test import TestTaskPayload
from featurebyte.worker.task.base import BaseTask


class TestTask(BaseTask):
    """
    Test Task
    """

    payload_class = TestTaskPayload

    async def execute(self) -> Any:
        """
        Execute test task
        """
        logger.debug("Test task started")
        for percent in range(0, 100, 20):
            self.update_progress(percent=percent)
        logger.debug("Test task completed")
