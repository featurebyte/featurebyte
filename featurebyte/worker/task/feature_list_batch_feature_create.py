"""
Feature list creation with batch feature creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.worker.task.batch_feature_create import BatchFeatureCreateTask


class FeatureListCreateWithBatchFeatureCreationTask(BatchFeatureCreateTask):
    """
    Feature list creation with batch feature creation task
    """

    payload_class: type[BaseTaskPayload] = FeatureListCreateWithBatchFeatureCreationTaskPayload

    async def execute(self) -> Any:
        """
        Execute Deployment Create & Update Task
        """
        payload = cast(FeatureListCreateWithBatchFeatureCreationTaskPayload, self.payload)

        # create list of features
        feature_ids = await self.batch_feature_create(
            payload=payload, start_percentage=0, end_percentage=90
        )

        # create feature list
        feature_list_create = FeatureListCreate(
            _id=payload.id, name=payload.name, feature_ids=feature_ids
        )
        await self.app_container.feature_list_controller.create_feature_list(
            data=feature_list_create
        )
        self.update_progress(percent=100, message="Completed feature list creation")
