"""
TargetTable creation task
"""
from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class TargetTableTask(DataWarehouseMixin, BaseTask):
    """
    TargetTableTask creates a TargetTable by computing historical features
    """

    payload_class = TargetTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute TargetTableTask
        """
        # TODO: will implement in a follow-up
        pass
