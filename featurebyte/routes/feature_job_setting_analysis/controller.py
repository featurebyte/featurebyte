"""
FeatureJobSettingAnalysis API route controller
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.models.base import FeatureByteBaseDocumentModel
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseController
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.task_status import TaskSubmission
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.task_manager import AbstractTaskManager


class FeatureJobSettingAnalysisController(
    BaseController[FeatureJobSettingAnalysisModel, FeatureJobSettingAnalysisList]
):
    """
    FeatureJobSettingAnalysis controller
    """

    collection_name = FeatureJobSettingAnalysisModel.collection_name()
    document_class = FeatureJobSettingAnalysisModel
    paginated_document_class = FeatureJobSettingAnalysisList

    @classmethod
    async def create_feature_job_setting_analysis(
        cls,
        user: Any,
        persistent: Persistent,
        task_manager: AbstractTaskManager,
        data: FeatureJobSettingAnalysisCreate,
    ) -> TaskSubmission:
        """
        Create Feature JobSetting Analysis and store in persisten

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        task_manager: AbstractTaskManager
            TaskManager to submit job to
        data: FeatureJobSettingAnalysisCreate
            FeatureJobSettingAnalysis creation payload

        Returns
        -------
        TaskSubmission
            TaskSubmission object for the submitted task
        """
        # check any conflict with existing documents
        output_document_id = data.id or ObjectId()
        await cls.check_document_unique_constraints(
            persistent=persistent,
            user_id=user.id,
            document=FeatureByteBaseDocumentModel(_id=output_document_id),
        )

        # check that event data exists
        _ = await cls.get_document(
            user=user,
            persistent=persistent,
            collection_name=EventDataModel.collection_name(),
            document_id=data.event_data_id,
        )

        payload = FeatureJobSettingAnalysisTaskPayload(
            **data.json_dict(), user_id=user.id, output_document_id=output_document_id
        )

        # run analysis
        task_id = await task_manager.submit(payload=payload)

        return TaskSubmission(
            task_id=task_id,
            output_document_id=payload.output_document_id,
        )
