"""
Feature Job Setting Analysis task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte_freeware.feature_job_analysis.analysis import (
    FeatureJobSettingsAnalysisResult,
    create_feature_job_settings_analysis,
)
from featurebyte_freeware.feature_job_analysis.database import EventDataset
from featurebyte_freeware.feature_job_analysis.schema import FeatureJobSetting

from featurebyte.logging import get_logger
from featurebyte.models.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisData,
    FeatureJobSettingAnalysisModel,
)
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBackTestTaskPayload,
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.session.manager import SessionManager
from featurebyte.worker.task.base import BaseTask

logger = get_logger(__name__)


class FeatureJobSettingAnalysisTask(BaseTask):
    """
    Feature Job Setting Analysis Task
    """

    payload_class = FeatureJobSettingAnalysisTaskPayload

    async def execute(self) -> Any:
        """
        Execute the task
        """
        self.update_progress(percent=0, message="Preparing data")
        payload = cast(FeatureJobSettingAnalysisTaskPayload, self.payload)
        persistent = self.get_persistent()

        # retrieve event data
        event_table_service = self.app_container.event_table_service
        event_table = await event_table_service.get_document(document_id=payload.event_table_id)

        # retrieve feature store
        feature_store_service = FeatureStoreService(
            user=self.user, persistent=persistent, catalog_id=self.payload.catalog_id
        )
        feature_store = await feature_store_service.get_document(
            document_id=event_table.tabular_source.feature_store_id
        )

        # establish database session
        session_manager = SessionManager(
            credentials={
                feature_store.name: await self.get_credential(
                    user_id=payload.user_id, feature_store_name=feature_store.name
                )
            }
        )
        db_session = await session_manager.get_session(feature_store)

        event_dataset = EventDataset(
            database_type=feature_store.type,
            event_table_name=event_table.name,
            table_details=event_table.tabular_source.table_details,
            creation_date_column=event_table.record_creation_timestamp_column,
            event_timestamp_column=event_table.event_timestamp_column,
            sql_query_func=db_session.execute_query,
        )

        self.update_progress(percent=5, message="Running Analysis")
        analysis = await create_feature_job_settings_analysis(
            event_dataset=event_dataset,
            **payload.dict(by_alias=True),
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

        self.update_progress(percent=95, message="Saving Analysis")
        feature_job_settings_analysis_service = (
            self.app_container.feature_job_setting_analysis_service
        )
        analysis_doc = await feature_job_settings_analysis_service.create_document(
            data=analysis_doc
        )
        assert analysis_doc.id == payload.output_document_id

        # store analysis data in storage
        analysis_data = FeatureJobSettingAnalysisData(**analysis.dict())
        await self.get_storage().put_object(
            analysis_data, f"feature_job_setting_analysis/{payload.output_document_id}/data.json"
        )

        logger.debug(
            "Completed feature job setting analysis",
            extra={"document_id": payload.output_document_id},
        )
        self.update_progress(percent=100, message="Analysis Completed")


class FeatureJobSettingAnalysisBacktestTask(BaseTask):
    """
    Feature Job Setting Analysis Task
    """

    payload_class = FeatureJobSettingAnalysisBackTestTaskPayload

    async def execute(self) -> None:
        """
        Execute the task
        """
        self.update_progress(percent=0, message="Preparing table")
        payload = cast(FeatureJobSettingAnalysisBackTestTaskPayload, self.payload)

        # retrieve analysis doc from persistent
        feature_job_settings_analysis_service = (
            self.app_container.feature_job_setting_analysis_service
        )
        analysis_doc = await feature_job_settings_analysis_service.get_document(
            document_id=payload.feature_job_setting_analysis_id
        )
        document = analysis_doc.dict(by_alias=True)

        # retrieve analysis data from storage
        storage = self.get_storage()
        analysis_data_raw = await storage.get_object(
            f"feature_job_setting_analysis/{payload.feature_job_setting_analysis_id}/data.json",
        )
        analysis_data = FeatureJobSettingAnalysisData(**analysis_data_raw).dict()

        # reconstruct analysis object
        analysis_result = analysis_data.pop("analysis_result")
        document.update(**analysis_data)
        document["analysis_result"].update(analysis_result)
        analysis = FeatureJobSettingsAnalysisResult.from_dict(document)

        # run backtest
        self.update_progress(percent=5, message="Running Analysis")
        backtest_result, backtest_report = analysis.backtest(
            FeatureJobSetting(
                frequency=payload.frequency,
                blind_spot=payload.blind_spot,
                job_time_modulo_frequency=payload.job_time_modulo_frequency,
                feature_cutoff_modulo_frequency=0,
            )
        )

        # store results in temp storage
        self.update_progress(percent=95, message="Saving Analysis")
        temp_storage = self.get_temp_storage()
        prefix = f"feature_job_setting_analysis/backtest/{payload.output_document_id}"
        await temp_storage.put_text(backtest_report, f"{prefix}.html")
        await temp_storage.put_dataframe(backtest_result.results, f"{prefix}.parquet")

        logger.debug(
            "Completed feature job setting analysis backtest",
            extra={"document_id": payload.output_document_id},
        )
        self.update_progress(percent=100, message="Analysis Completed")
