"""
Feature Job Setting Analysis task
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from featurebyte_freeware.feature_job_analysis.analysis import create_feature_job_settings_analysis
from featurebyte_freeware.feature_job_analysis.database import DatabaseTableDetails, EventDataset

from featurebyte import SourceType
from featurebyte.logging import get_logger
from featurebyte.models.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisData,
    FeatureJobSettingAnalysisModel,
)
from featurebyte.schema.feature_job_setting_analysis import EventTableCandidate
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class FeatureJobSettingAnalysisTask(BaseTask[FeatureJobSettingAnalysisTaskPayload]):
    """
    Feature Job Setting Analysis Task
    """

    payload_class = FeatureJobSettingAnalysisTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        storage: Storage,
        event_table_service: EventTableService,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.storage = storage
        self.event_table_service = event_table_service
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.feature_job_setting_analysis_service = feature_job_setting_analysis_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: FeatureJobSettingAnalysisTaskPayload) -> str:
        # retrieve event data
        if payload.event_table_id:
            event_table_document = await self.event_table_service.get_document(
                document_id=payload.event_table_id
            )
            event_table_name = event_table_document.name
        else:
            # event table candidate should be provided if event table is not
            assert payload.event_table_candidate
            event_table_name = payload.event_table_candidate.name
        return f'Analyze feature job settings for table "{event_table_name}"'

    async def execute(self, payload: FeatureJobSettingAnalysisTaskPayload) -> Any:
        await self.task_progress_updater.update_progress(percent=0, message="Preparing data")

        # retrieve event data
        if payload.event_table_id:
            event_table_document = await self.event_table_service.get_document(
                document_id=payload.event_table_id
            )
            event_table = EventTableCandidate(
                name=event_table_document.name,
                tabular_source=event_table_document.tabular_source,
                record_creation_timestamp_column=event_table_document.record_creation_timestamp_column,
                event_timestamp_column=event_table_document.event_timestamp_column,
            )
        else:
            # event table candidate should be provided if event table is not
            assert payload.event_table_candidate
            event_table = payload.event_table_candidate

        # retrieve feature store
        feature_store = await self.feature_store_service.get_document(
            document_id=event_table.tabular_source.feature_store_id
        )

        # establish database session
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        database_type = feature_store.type
        if database_type in {SourceType.DATABRICKS, SourceType.DATABRICKS_UNITY}:
            database_type = SourceType.SPARK

        table_details = DatabaseTableDetails(
            database_name=event_table.tabular_source.table_details.database_name,
            schema_name=event_table.tabular_source.table_details.schema_name,
            table_name=event_table.tabular_source.table_details.table_name,
        )
        event_dataset = EventDataset(
            database_type=database_type,
            event_table_name=event_table.name,
            table_details=table_details,
            creation_date_column=event_table.record_creation_timestamp_column,
            event_timestamp_column=event_table.event_timestamp_column,
            sql_query_func=db_session.execute_query,
        )

        await self.task_progress_updater.update_progress(percent=5, message="Running Analysis")
        analysis = await create_feature_job_settings_analysis(
            event_dataset=event_dataset,
            **payload.model_dump(by_alias=True),
        )

        # store analysis doc in persistent
        analysis_doc = FeatureJobSettingAnalysisModel(
            _id=payload.output_document_id,
            user_id=payload.user_id,
            name=payload.name,
            event_table_id=payload.event_table_id,
            analysis_options=analysis.analysis_options.dict(),
            analysis_parameters=analysis.analysis_parameters.dict(),
            analysis_result=analysis.analysis_result.dict(),
            analysis_report=analysis.to_html(),
        )

        await self.task_progress_updater.update_progress(percent=95, message="Saving Analysis")
        analysis_doc = await self.feature_job_setting_analysis_service.create_document(
            data=analysis_doc
        )
        assert analysis_doc.id == payload.output_document_id

        # store analysis data in storage
        analysis_data = FeatureJobSettingAnalysisData(**analysis.dict())
        await self.storage.put_object(
            analysis_data,
            Path(f"feature_job_setting_analysis/{payload.output_document_id}/data.json"),
        )

        logger.debug(
            "Completed feature job setting analysis",
            extra={"document_id": payload.output_document_id},
        )
        await self.task_progress_updater.update_progress(percent=100, message="Analysis Completed")
