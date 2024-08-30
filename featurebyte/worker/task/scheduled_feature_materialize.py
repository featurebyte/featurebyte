"""
Scheduled feature materialize task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.scheduled_feature_materialize import (
    ScheduledFeatureMaterializeTaskPayload,
)
from featurebyte.service.feature_materialize import FeatureMaterializeService
from featurebyte.service.offline_store_feature_table import OfflineStoreFeatureTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class ScheduledFeatureMaterializeTask(BaseTask[ScheduledFeatureMaterializeTaskPayload]):
    """
    ScheduledFeatureMaterializeTask class
    """

    payload_class = ScheduledFeatureMaterializeTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        offline_store_feature_table_service: OfflineStoreFeatureTableService,
        feature_materialize_service: FeatureMaterializeService,
    ):
        super().__init__(task_manager=task_manager)
        self.offline_store_feature_table_service = offline_store_feature_table_service
        self.feature_materialize_service = feature_materialize_service

    async def get_task_description(self, payload: ScheduledFeatureMaterializeTaskPayload) -> str:
        return f'Materialize features for offline store table "{payload.offline_store_feature_table_name}"'

    async def execute(self, payload: ScheduledFeatureMaterializeTaskPayload) -> Any:
        feature_table = await self.offline_store_feature_table_service.get_document(
            payload.offline_store_feature_table_id
        )
        await self.feature_materialize_service.scheduled_materialize_features(
            feature_table_model=feature_table,
            feature_materialize_run_id=payload.feature_materialize_run_id,
        )
