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
from featurebyte.query_graph.model.feature_job_setting import FeatureJobSetting
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.feature_job_setting_analysis import (
    EventTableCandidate,
    FeatureJobSettingAnalysisBacktest,
    FeatureJobSettingAnalysisCreate,
    FeatureJobSettingAnalysisList,
)
from featurebyte.schema.info import FeatureJobSettingAnalysisInfo
from featurebyte.schema.task import Task
from featurebyte.service.catalog import CatalogService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService


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
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
        task_controller: TaskController,
        event_table_service: EventTableService,
        catalog_service: CatalogService,
    ):
        super().__init__(feature_job_setting_analysis_service)
        self.task_controller = task_controller
        self.event_table_service = event_table_service
        self.catalog_service = catalog_service

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
        _ = verbose
        feature_job_setting_analysis = await self.service.get_document(document_id=document_id)
        recommended_setting = (
            feature_job_setting_analysis.analysis_result.recommended_feature_job_setting
        )
        if feature_job_setting_analysis.event_table_id:
            event_table_doc = await self.event_table_service.get_document(
                document_id=feature_job_setting_analysis.event_table_id
            )
            event_table = EventTableCandidate(
                name=event_table_doc.name,
                tabular_source=event_table_doc.tabular_source,
                record_creation_timestamp_column=event_table_doc.record_creation_timestamp_column,
                event_timestamp_column=event_table_doc.event_timestamp_column,
            )
        else:
            assert feature_job_setting_analysis.event_table_candidate is not None
            event_table = feature_job_setting_analysis.event_table_candidate

        # get catalog info
        catalog = await self.catalog_service.get_document(feature_job_setting_analysis.catalog_id)

        return FeatureJobSettingAnalysisInfo(
            created_at=feature_job_setting_analysis.created_at,
            event_table_name=event_table.name,
            analysis_options=feature_job_setting_analysis.analysis_options,
            analysis_parameters=feature_job_setting_analysis.analysis_parameters,
            recommendation=FeatureJobSetting(
                blind_spot=f"{recommended_setting.blind_spot}s",
                offset=f"{recommended_setting.offset}s",
                period=f"{recommended_setting.period}s",
            ),
            catalog_name=catalog.name,
            description=catalog.description,
        )

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

    async def backtest(self, data: FeatureJobSettingAnalysisBacktest) -> Task:
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
