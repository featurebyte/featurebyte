"""
FeatureJobSettingAnalysisService class
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.exception import DocumentError
from featurebyte.models.base import FeatureByteBaseDocumentModel, FeatureByteBaseModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
)
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBackTestTaskPayload,
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.task_manager import AbstractTaskManager, TaskId


class FeatureJobSettingAnalysisService(
    BaseDocumentService[FeatureJobSettingAnalysisModel, FeatureByteBaseModel, FeatureByteBaseModel]
):
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
        """
        Create document creation task

        Parameters
        ----------
        data: FeatureJobSettingAnalysisCreate
            FeatureJobSettingAnalysis creation payload
        task_manager: AbstractTaskManager
            TaskManager

        Returns
        -------
        TaskId

        Raises
        ------
        DocumentError
            Creation date column is not available for the event data
        """
        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await self._check_document_unique_constraints(
            document=FeatureByteBaseDocumentModel(_id=output_document_id),
        )

        # check that event data exists
        event_data_service = EventDataService(
            user=self.user,
            persistent=self.persistent,
        )
        event_data = await event_data_service.get_document(
            document_id=data.event_data_id,
            collection_name=EventDataModel.collection_name(),
        )
        if not event_data.record_creation_date_column:
            raise DocumentError("Creation date column is not available for the event data.")

        payload = FeatureJobSettingAnalysisTaskPayload(
            **data.dict(), user_id=self.user.id, output_document_id=output_document_id
        )

        # submit a task to run analysis
        return await task_manager.submit(payload=payload)

    async def create_backtest_task(
        self, data: FeatureJobSettingAnalysisBacktest, task_manager: AbstractTaskManager
    ) -> TaskId:
        """
        Create document creation task

        Parameters
        ----------
        data: FeatureJobSettingAnalysisBacktest
            FeatureJobSettingAnalysis backtest payload
        task_manager: AbstractTaskManager
            TaskManager

        Returns
        -------
        TaskId
        """
        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()

        # check that analysis exists
        _ = await self.get_document(
            document_id=data.feature_job_setting_analysis_id,
        )

        payload = FeatureJobSettingAnalysisBackTestTaskPayload(
            **data.dict(), user_id=self.user.id, output_document_id=output_document_id
        )

        # submit a task to run analysis
        return await task_manager.submit(payload=payload)
