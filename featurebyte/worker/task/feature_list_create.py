"""
Feature list creation task
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from bson import ObjectId, json_util

from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.feature_list_create import (
    FeatureListCreateTaskPayload,
    FeatureParameters,
    FeaturesParameters,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.batch_feature_creator import BatchFeatureCreator
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater


class FeatureListCreateTask(BaseTask[FeatureListCreateTaskPayload]):
    """
    Feature list creation task
    """

    payload_class = FeatureListCreateTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_list_controller: FeatureListController,
        batch_feature_creator: BatchFeatureCreator,
        feature_service: FeatureService,
        storage: Storage,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_list_controller = feature_list_controller
        self.batch_feature_creator = batch_feature_creator
        self.feature_service = feature_service
        self.storage = storage
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: FeatureListCreateTaskPayload) -> str:
        return f'Save feature list "{payload.feature_list_name}"'

    async def execute(self, payload: FeatureListCreateTaskPayload) -> Any:
        features_parameters_data = await self.storage.get_text(
            Path(payload.features_parameters_path)
        )
        features_parameters = FeaturesParameters(**json_util.loads(features_parameters_data))

        # extract feature ids from payload
        request_feature_ids, feature_names = [], []
        for feature in features_parameters.features:
            if isinstance(feature, FeatureParameters):
                request_feature_ids.append(feature.id)
                feature_names.append(feature.name)
            elif isinstance(feature, ObjectId):
                request_feature_ids.append(feature)

        saved_feature_ids = await self.batch_feature_creator.identify_saved_feature_ids(
            feature_ids=request_feature_ids
        )
        conflict_to_resolution_feature_id_map = (
            await self.batch_feature_creator.get_conflict_to_resolution_feature_id_mapping(
                conflict_resolution=payload.features_conflict_resolution,
                feature_names=feature_names,
            )
        )
        feature_ids = []
        for feature in features_parameters.features:
            if isinstance(feature, FeatureParameters):
                feature_id = feature.id
                feature_name = feature.name
            else:
                feature_id = feature
                feature_name = None

            if feature_id in saved_feature_ids:
                feature_ids.append(feature_id)
            else:
                if feature_name not in conflict_to_resolution_feature_id_map:
                    # retrieve non-existing feature to trigger a not found error
                    await self.feature_service.get_document(document_id=feature_id)
                assert feature_name is not None
                feature_ids.append(conflict_to_resolution_feature_id_map[feature_name])

        # create feature list
        feature_list_create = FeatureListCreate(
            _id=payload.feature_list_id, name=payload.feature_list_name, feature_ids=feature_ids
        )
        await self.feature_list_controller.create_feature_list(
            data=feature_list_create,
            progress_callback=self.task_progress_updater.update_progress,
        )
        # cleanup the storage file
        await self.storage.delete(remote_path=Path(payload.features_parameters_path))
        await self.task_progress_updater.update_progress(
            percent=100, message="Completed feature list creation"
        )
