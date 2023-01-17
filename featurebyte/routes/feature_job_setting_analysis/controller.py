"""
FeatureJobSettingAnalysis API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.info import FeatureJobSettingAnalysisInfo
from featurebyte.schema.task import Task
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.info import InfoService


class FeatureJobSettingAnalysisController(
    BaseDocumentController[
        FeatureJobSettingAnalysisModel,
        FeatureJobSettingAnalysisService,
        FeatureJobSettingAnalysisList,
    ]
):
    """
    FeatureJobSettingAnalysis controller
    """

    paginated_document_class = FeatureJobSettingAnalysisList

    def __init__(
        self,
        service: FeatureJobSettingAnalysisService,
        task_controller: TaskController,
        info_service: InfoService,
    ):
        super().__init__(service)
        self.task_controller = task_controller
        self.info_service = info_service

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> FeatureJobSettingAnalysisInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        FeatureJobSettingAnalysisInfo
        """
        info_document = await self.info_service.get_feature_job_setting_analysis_info(
            document_id=document_id, verbose=verbose
        )
        return info_document

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
        task_id = await self.service.create_document_creation_task(
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
        task_id = await self.service.create_backtest_task(
            data=data, task_manager=self.task_controller.task_manager
        )
        return await self.task_controller.get_task(task_id=str(task_id))
