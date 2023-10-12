"""
Feature list creation with batch feature creation task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.models.base import User
from featurebyte.persistent import Persistent
from featurebyte.routes.feature.controller import FeatureController
from featurebyte.routes.feature_list.controller import FeatureListController
from featurebyte.schema.feature_list import FeatureListCreate
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.schema.worker.task.feature_list_batch_feature_create import (
    FeatureListCreateWithBatchFeatureCreationTaskPayload,
)
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_namespace import FeatureNamespaceService
from featurebyte.service.namespace_handler import NamespaceHandler
from featurebyte.storage import Storage
from featurebyte.worker.task.batch_feature_create import BatchFeatureCreateTask


class FeatureListCreateWithBatchFeatureCreationTask(BatchFeatureCreateTask):
    """
    Feature list creation with batch feature creation task
    """

    payload_class: type[BaseTaskPayload] = FeatureListCreateWithBatchFeatureCreationTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
        feature_service: FeatureService,
        feature_namespace_service: FeatureNamespaceService,
        feature_controller: FeatureController,
        namespace_handler: NamespaceHandler,
        feature_list_controller: FeatureListController,
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
            storage=storage,
            temp_storage=temp_storage,
            feature_service=feature_service,
            feature_namespace_service=feature_namespace_service,
            feature_controller=feature_controller,
            namespace_handler=namespace_handler,
        )
        self.feature_list_controller = feature_list_controller

    async def get_task_description(self) -> str:
        payload = cast(FeatureListCreateWithBatchFeatureCreationTaskPayload, self.payload)
        return f'Save feature list "{payload.name}"'

    async def execute(self) -> Any:
        """
        Execute FeatureListCreateWithBatchFeatureCreationTask
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
        await self.feature_list_controller.create_feature_list(data=feature_list_create)
        await self.update_progress(percent=100, message="Completed feature list creation")
