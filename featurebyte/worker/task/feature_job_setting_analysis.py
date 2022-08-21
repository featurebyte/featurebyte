"""
Feature Job Setting Analysis task
"""
from __future__ import annotations

from typing import cast

from featurebyte_freeware.feature_job_analysis.analysis import create_feature_job_settings_analysis
from featurebyte_freeware.feature_job_analysis.database import EventDataset

from featurebyte.core.generic import ExtendedFeatureStoreModel
from featurebyte.logger import logger
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_job_setting_analysis import FeatureJobSettingAnalysisModel
from featurebyte.schema.worker.task.feature_job_setting_analysis import (
    FeatureJobSettingAnalysisTaskPayload,
)
from featurebyte.worker.task.base import BaseTask


class FeatureJobSettingAnalysisTask(BaseTask):
    """
    Feature Job Setting Analysis Task
    """

    payload_class = FeatureJobSettingAnalysisTaskPayload

    async def execute(self) -> None:
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
        query_filter = {"_id": event_data.tabular_source[0]}
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
            table_details=event_data.tabular_source[1].dict(),
            creation_date_column=event_data.record_creation_date_column,
            event_timestamp_column=event_data.event_timestamp_column,
            sql_query_func=db_session.execute_query,
        )

        self.update_progress(percent=5, message="Running Analysis")
        analysis = create_feature_job_settings_analysis(
            event_dataset=event_dataset,
            **payload.json_dict(),
        )

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

        logger.debug(
            "Completed feature job setting analysis",
            extra={"document_id": payload.output_document_id},
        )
        self.update_progress(percent=100, message="Analysis Completed")
