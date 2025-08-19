"""
Feature store table cleanup task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.feature_store_table_cleanup import (
    FeatureStoreTableCleanupTaskPayload,
)
from featurebyte.service.feature_store_table_cleanup import FeatureStoreTableCleanupService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class FeatureStoreTableCleanupTask(BaseTask[FeatureStoreTableCleanupTaskPayload]):
    """
    FeatureStoreTableCleanupTask class
    """

    payload_class = FeatureStoreTableCleanupTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_table_cleanup_service: FeatureStoreTableCleanupService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_table_cleanup_service = feature_store_table_cleanup_service

    async def get_task_description(self, payload: FeatureStoreTableCleanupTaskPayload) -> str:
        return f'Clean up feature store tables for feature store "{payload.feature_store_id}"'

    async def execute(self, payload: FeatureStoreTableCleanupTaskPayload) -> Any:
        await self.feature_store_table_cleanup_service.run_cleanup(
            feature_store_id=payload.feature_store_id,
        )
