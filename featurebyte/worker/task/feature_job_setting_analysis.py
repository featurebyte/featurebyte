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

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.logger import logger
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisData,
    FeatureJobSettingAnalysisModel,
)
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisBackTestTaskPayload,
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.worker.task.base import BaseTask


class FeatureJobSettingAnalysisTask(BaseTask):
    """
    Feature Job Setting Analysis Task
    """

    payload_class = FeatureJobSettingAnalysisTaskPayload

    async def execute(self) -> Any:
        """
        Execute the task

        Raises
        ------
        ValueError
            Event data or feature store records not found
        """
        self.update_progress(percent=0, message="Preparing data")
        payload = cast(FeatureJobSettingAnalysisTaskPayload, self.payload)
        persistent = self.get_persistent()

        # retrieve event data
        query_filter = {"_id": payload.event_data_id}
        document = await persistent.find_one(
            collection_name=EventDataModel.collection_name(),
            query_filter=query_filter,
            user_id=payload.user_id,
        )
        if not document:
            message = "Event Data not found"
            logger.error(message, extra={"query_filter": query_filter})
            raise ValueError(message)

        event_data = EventDataModel(**document)

        # retrieve feature store
        query_filter = {"_id": event_data.tabular_source.feature_store_id}
        document = await persistent.find_one(
            collection_name=ExtendedFeatureStoreModel.collection_name(),
            query_filter=query_filter,
            user_id=payload.user_id,
        )
        if not document:
            message = "Feature Store not found"
            logger.error(message, extra={"query_filter": query_filter})
            raise ValueError(message)
        feature_store = ExtendedFeatureStoreModel(**document)

        # establish database session
        db_session = feature_store.get_session(
            credentials={
                feature_store.name: await self.get_credential(
                    user_id=payload.user_id, feature_store_name=feature_store.name
                )
            }
        )

        # create analysis
        event_dataset = EventDataset(
            database_type=feature_store.type,
            event_data_name=event_data.name,
            table_details=event_data.tabular_source.table_details.dict(),
            creation_date_column=event_data.record_creation_date_column,
            event_timestamp_column=event_data.event_timestamp_column,
            sql_query_func=db_session.execute_query,
        )

        self.update_progress(percent=5, message="Running Analysis")
        analysis = create_feature_job_settings_analysis(
            event_dataset=event_dataset,
            **payload.json_dict(),
        )

        # store analysis doc in persistent
        analysis_doc = FeatureJobSettingAnalysisModel(
            _id=payload.output_document_id,
            user_id=payload.user_id,
            name=payload.name,
            event_data_id=payload.event_data_id,
            analysis_options=analysis.analysis_options,
            analysis_parameters=analysis.analysis_parameters,
            analysis_result=analysis.analysis_result,
            analysis_report=analysis.to_html(),
        )

        self.update_progress(percent=95, message="Saving Analysis")
        insert_id = await persistent.insert_one(
            collection_name=FeatureJobSettingAnalysisModel.collection_name(),
            document=analysis_doc.dict(by_alias=True),
            user_id=payload.user_id,
        )
        assert insert_id == payload.output_document_id

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

        Raises
        ------
        ValueError
            Event data or feature store records not found
        """
        self.update_progress(percent=0, message="Preparing data")
        payload = cast(FeatureJobSettingAnalysisBackTestTaskPayload, self.payload)
        persistent = self.get_persistent()

        # retrieve analysis doc from persistent
        query_filter = {"_id": payload.feature_job_setting_analysis_id}
        document = await persistent.find_one(
            collection_name=FeatureJobSettingAnalysisModel.collection_name(),
            query_filter=query_filter,
            user_id=payload.user_id,
        )
        if not document:
            message = "Feature Job Setting Analysis not found"
            logger.error(message, extra={"query_filter": query_filter})
            raise ValueError(message)

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
        analysis = FeatureJobSettingsAnalysisResult(**document)

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
