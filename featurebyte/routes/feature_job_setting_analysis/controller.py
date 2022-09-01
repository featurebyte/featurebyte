"""
FeatureJobSettingAnalysis API route controller
"""
from __future__ import annotations

from typing import Any, Type

from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.task import Task
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.task_manager import AbstractTaskManager


class FeatureJobSettingAnalysisController(
    BaseDocumentController[FeatureJobSettingAnalysisModel, FeatureJobSettingAnalysisList]
):
    """
    FeatureJobSettingAnalysis controller
    """

    paginated_document_class = FeatureJobSettingAnalysisList
    document_service_class: Type[
        FeatureJobSettingAnalysisService
    ] = FeatureJobSettingAnalysisService  # type: ignore[assignment]

    @classmethod
    async def create_feature_job_setting_analysis(
        cls,
        user: Any,
        persistent: Persistent,
        task_manager: AbstractTaskManager,
        data: FeatureJobSettingAnalysisCreate,
    ) -> Task:
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
        Task
            Task object for the submitted task
        """
        async with cls._creation_context():
            task_id = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document_creation_task(data=data, task_manager=task_manager)
            return await TaskController.get_task(task_manager=task_manager, task_id=str(task_id))
