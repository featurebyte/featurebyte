"""
Feature list make production ready task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.models.base import User
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.task.base import BaseTaskPayload
from featurebyte.schema.worker.task.feature_list_make_production_ready import (
    FeatureListMakeProductionReadyTaskPayload,
)
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_facade import FeatureListFacadeService
from featurebyte.worker.task.base import BaseTask


class FeatureListMakeProductionReadyTask(BaseTask):
    """
    Feature list make production ready task
    """

    payload_class: type[BaseTaskPayload] = FeatureListMakeProductionReadyTaskPayload

    def __init__(
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        feature_list_service: FeatureListService,
        feature_list_facade_service: FeatureListFacadeService,
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
        )
        self.feature_list_service = feature_list_service
        self.feature_list_facade_service = feature_list_facade_service

    async def get_task_description(self) -> str:
        payload = cast(FeatureListMakeProductionReadyTaskPayload, self.payload)
        feature_list_doc = await self.feature_list_service.get_document_as_dict(
            document_id=payload.feature_list_id,
        )
        feature_list_name = feature_list_doc["name"]
        return f'Make all features of feature list "{feature_list_name}" production ready'

    async def execute(self) -> Any:
        """
        Execute FeatureListMakeProductionReadyTask
        """
        assert isinstance(self.payload, FeatureListMakeProductionReadyTaskPayload)
        await self.feature_list_facade_service.make_features_production_ready(
            feature_list_id=self.payload.feature_list_id,
            ignore_guardrails=self.payload.ignore_guardrails,
        )
