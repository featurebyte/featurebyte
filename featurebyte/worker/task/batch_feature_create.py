"""
Batch feature create task
"""
from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.schema.worker.task.batch_feature_create import BatchFeatureCreateTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.batch_feature_creator import BatchFeatureCreator

logger = get_logger(__name__)


class BatchFeatureCreateTask(BaseTask[BatchFeatureCreateTaskPayload]):
    """
    Batch feature creation task
    """

    payload_class = BatchFeatureCreateTaskPayload

    def __init__(
        self,
        batch_feature_creator: BatchFeatureCreator,
    ):
        super().__init__()
        self.batch_feature_creator = batch_feature_creator

    async def get_task_description(self, payload: BatchFeatureCreateTaskPayload) -> str:
        return f"Save {len(payload.features)} features"

    async def execute(self, payload: BatchFeatureCreateTaskPayload) -> Any:
        await self.batch_feature_creator.batch_feature_create(
            payload=payload, start_percentage=0, end_percentage=100
        )
