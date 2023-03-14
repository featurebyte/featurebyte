"""
FeatureJobSettingAnalysis API route controller
"""
from __future__ import annotations

import tempfile
from io import BytesIO

import pdfkit
from bson import ObjectId
from fastapi.responses import StreamingResponse

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
        # submit a task to run analysis
        payload = await self.service.create_document_creation_task(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
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
        payload = await self.service.create_backtest_task(data=data)
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_feature_job_setting_analysis_report(
        self, feature_job_setting_analysis_id: ObjectId
    ) -> StreamingResponse:
        """
        Retrieve analysis report as pdf file

        Parameters
        ----------
        feature_job_setting_analysis_id: ObjectId
            FeatureJobSettingAnalysis Id

        Returns
        -------
        StreamingResponse
        """
        analysis: FeatureJobSettingAnalysisModel = await self.service.get_document(
            document_id=feature_job_setting_analysis_id
        )
        buffer = BytesIO()

        options = {"page-size": "Letter", "encoding": "UTF-8", "no-outline": None}
        with tempfile.NamedTemporaryFile() as file_obj:
            pdfkit.from_string(analysis.analysis_report, file_obj.name, options=options)
            file_obj.seek(0)
            buffer.write(file_obj.read())
            buffer.seek(0)
        return StreamingResponse(
            buffer,
            media_type="application/pdf",
            headers={
                "content-disposition": (
                    'attachment; name="report"; '
                    f'filename="feature_job_setting_analysis_{feature_job_setting_analysis_id}.pdf"'
                )
            },
        )
