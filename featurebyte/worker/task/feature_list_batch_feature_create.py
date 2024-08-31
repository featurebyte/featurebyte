"""
Feature list creation with batch feature creation task
"""

from __future__ import annotations

from typing import Any, Sequence

from bson import ObjectId

from featurebyte.common.progress import get_ranged_progress_callback
from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.batch_feature_creator import (
    BatchFeatureCreator,
    patch_api_object_cache,
)
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


class FeatureListCreateWithBatchFeatureCreationTask(
    BaseTask[FeatureListCreateWithBatchFeatureCreationTaskPayload]
):
    """
    Feature list creation with batch feature creation task
    """

    payload_class = FeatureListCreateWithBatchFeatureCreationTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_list_controller: FeatureListController,
        batch_feature_creator: BatchFeatureCreator,
        feature_service: FeatureService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_list_controller = feature_list_controller
        self.batch_feature_creator = batch_feature_creator
        self.feature_service = feature_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(
        self, payload: FeatureListCreateWithBatchFeatureCreationTaskPayload
    ) -> str:
        return f'Save feature list "{payload.name}"'

    @patch_api_object_cache()
    async def execute(self, payload: FeatureListCreateWithBatchFeatureCreationTaskPayload) -> Any:
        feature_ids: Sequence[ObjectId]
        if payload.skip_batch_feature_creation:
            # extract feature ids from payload
            request_feature_ids, feature_names = [], []
            for feature in payload.features:
                request_feature_ids.append(feature.id)
                feature_names.append(feature.name)
            saved_feature_ids = await self.batch_feature_creator.identify_saved_feature_ids(
                feature_ids=request_feature_ids
            )
            conflict_to_resolution_feature_id_map = (
                await self.batch_feature_creator.get_conflict_to_resolution_feature_id_mapping(
                    conflict_resolution=payload.conflict_resolution, feature_names=feature_names
                )
            )
            feature_ids = []
            for feature in payload.features:
                if feature.id in saved_feature_ids:
                    feature_ids.append(feature.id)
                else:
                    if feature.name not in conflict_to_resolution_feature_id_map:
                        # retrieve non-existing feature to trigger a not found error
                        await self.feature_service.get_document(document_id=feature.id)
                    feature_ids.append(conflict_to_resolution_feature_id_map[feature.name])
        else:
            # create list of features
            feature_ids = await self.batch_feature_creator.batch_feature_create(
                payload=payload, start_percentage=0, end_percentage=90
            )

        # create feature list
        feature_list_create = FeatureListCreate(
            _id=payload.id, name=payload.name, feature_ids=feature_ids
        )
        await self.feature_list_controller.create_feature_list(
            data=feature_list_create,
            progress_callback=get_ranged_progress_callback(
                self.task_progress_updater.update_progress, 90, 100
            ),
        )
        await self.task_progress_updater.update_progress(
            percent=100, message="Completed feature list creation"
        )
