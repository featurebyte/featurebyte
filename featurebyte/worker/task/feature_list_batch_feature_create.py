"""
Feature list creation with batch feature creation task
"""
from __future__ import annotations

from typing import Any

from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.batch_feature_creator import BatchFeatureCreator
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


class FeatureListCreateWithBatchFeatureCreationTask(
    BaseTask[FeatureListCreateWithBatchFeatureCreationTaskPayload]
):
    """
    Feature list creation with batch feature creation task
    """

    payload_class = FeatureListCreateWithBatchFeatureCreationTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        feature_list_controller: FeatureListController,
        batch_feature_creator: BatchFeatureCreator,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__()
        self.feature_list_controller = feature_list_controller
        self.batch_feature_creator = batch_feature_creator
        self.task_progress_updater = task_progress_updater

    async def get_task_description(
        self, payload: FeatureListCreateWithBatchFeatureCreationTaskPayload
    ) -> str:
        return f'Save feature list "{payload.name}"'

    async def execute(self, payload: FeatureListCreateWithBatchFeatureCreationTaskPayload) -> Any:
        # create list of features
        feature_ids = await self.batch_feature_creator.batch_feature_create(
            payload=payload, start_percentage=0, end_percentage=90
        )

        # create feature list
        feature_list_create = FeatureListCreate(
            _id=payload.id, name=payload.name, feature_ids=feature_ids
        )
        await self.feature_list_controller.create_feature_list(data=feature_list_create)
        await self.task_progress_updater.update_progress(
            percent=100, message="Completed feature list creation"
        )
