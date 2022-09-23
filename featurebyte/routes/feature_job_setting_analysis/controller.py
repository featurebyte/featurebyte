"""
FeatureJobSettingAnalysis API route controller
"""
from __future__ import annotations

from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.task import Task
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService


class FeatureJobSettingAnalysisController(
    BaseDocumentController[FeatureJobSettingAnalysisModel, FeatureJobSettingAnalysisList]
):
    """
    FeatureJobSettingAnalysis controller
    """

    paginated_document_class = FeatureJobSettingAnalysisList

    def __init__(
        self,
        service: FeatureJobSettingAnalysisService,
        task_controller: TaskController,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.task_controller = task_controller

    async def create_feature_job_setting_analysis(
        self,
        data: FeatureJobSettingAnalysisCreate,
    ) -> Task:
        """
        Create Feature JobSetting Analysis and store in persistent

        Parameters
        ----------
        data: FeatureJobSettingAnalysisCreate
            FeatureJobSettingAnalysis creation payload

        Returns
        -------
        Task
            Task object for the submitted task
        """
        task_id = await self.service.create_document_creation_task(  # type: ignore[attr-defined]
            data=data, task_manager=self.task_controller.task_manager
        )
        return await self.task_controller.get_task(task_id=str(task_id))

    async def backtest(
        self,
        data: FeatureJobSettingAnalysisBacktest,
    ) -> Task:
        """
        Run backtest on a Feature JobSetting Analysis

        Parameters
        ----------
        data: FeatureJobSettingAnalysisBacktest
            FeatureJobSettingAnalysis backtest payload

        Returns
        -------
        Task
            Task object for the submitted task
        """
        task_id = await self.service.create_backtest_task(  # type: ignore[attr-defined]
            data=data, task_manager=self.task_controller.task_manager
        )
        return await self.task_controller.get_task(task_id=str(task_id))
