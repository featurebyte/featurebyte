"""
FeatureJobSettingAnalysisService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.feature_job_setting_analysis import FeatureJobSettingAnalysisCreate
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.task_manager import AbstractTaskManager, TaskId


class FeatureJobSettingAnalysisService(BaseDocumentService[FeatureJobSettingAnalysisModel]):
    """
    FeatureJobSettingAnalysisService class
    """

    document_class = FeatureJobSettingAnalysisModel

    async def create_document(
        self, data: FeatureByteBaseModel, get_credential: Any = None
    ) -> FeatureJobSettingAnalysisModel:
        raise NotImplementedError

    async def create_document_creation_task(
        self, data: FeatureJobSettingAnalysisCreate, task_manager: AbstractTaskManager
    ) -> TaskId:
        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id),
        )

        # check that event data exists
        _ = await self._get_document(
            document_id=data.event_data_id,
            collection_name=EventDataModel.collection_name(),
        )

        payload = FeatureJobSettingAnalysisTaskPayload(
            **data.dict(), user_id=self.user.id, output_document_id=output_document_id
        )

        # submit a task to run analysis
        return await task_manager.submit(payload=payload)

    async def update_document(
        self, document_id: ObjectId, data: FeatureByteBaseModel
    ) -> FeatureJobSettingAnalysisModel:
        # TODO: implement proper logic to update feature job analysis document
        return await self.get_document(document_id=document_id)
