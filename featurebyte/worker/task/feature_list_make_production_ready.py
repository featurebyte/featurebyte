"""
Feature list make production ready task
"""
from __future__ import annotations

from typing import Any

from featurebyte.schema.worker.task.feature_list_make_production_ready import (
    FeatureListMakeProductionReadyTaskPayload,
)
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_list_facade import FeatureListFacadeService
from featurebyte.worker.task.base import BaseTask


class FeatureListMakeProductionReadyTask(BaseTask[FeatureListMakeProductionReadyTaskPayload]):
    """
    Feature list make production ready task
    """

    payload_class = FeatureListMakeProductionReadyTaskPayload

    def __init__(
        self,
        feature_list_service: FeatureListService,
        feature_list_facade_service: FeatureListFacadeService,
    ):
        super().__init__()
        self.feature_list_service = feature_list_service
        self.feature_list_facade_service = feature_list_facade_service

    async def get_task_description(self, payload: FeatureListMakeProductionReadyTaskPayload) -> str:
        feature_list_doc = await self.feature_list_service.get_document_as_dict(
            document_id=payload.feature_list_id,
            projection={"name": 1},
        )
        feature_list_name = feature_list_doc["name"]
        return f'Make all features of feature list "{feature_list_name}" production ready'

    async def execute(self, payload: FeatureListMakeProductionReadyTaskPayload) -> Any:
        await self.feature_list_facade_service.make_features_production_ready(
            feature_list_id=payload.feature_list_id,
            ignore_guardrails=payload.ignore_guardrails,
        )
