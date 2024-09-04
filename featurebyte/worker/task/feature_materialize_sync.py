"""
FeatureMaterializeSyncTask class
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.feature_materialize_sync import (
    FeatureMaterializeSyncTaskPayload,
)
from featurebyte.service.feature_materialize_sync import FeatureMaterializeSyncService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class FeatureMaterializeSyncTask(BaseTask[FeatureMaterializeSyncTaskPayload]):
    """
    FeatureMaterializeSyncTask class
    """

    payload_class = FeatureMaterializeSyncTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_materialize_sync_service: FeatureMaterializeSyncService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_materialize_sync_service = feature_materialize_sync_service

    async def get_task_description(self, payload: FeatureMaterializeSyncTaskPayload) -> str:
        return f'Synchronize feature materialize tasks for offline store table "{payload.offline_store_feature_table_name}"'

    async def execute(self, payload: FeatureMaterializeSyncTaskPayload) -> Any:
        await self.feature_materialize_sync_service.run_feature_materialize(
            offline_store_feature_table_id=payload.offline_store_feature_table_id
        )
