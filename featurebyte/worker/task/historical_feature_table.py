"""
HistoricalFeatureTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.schema.worker.task.historical_feature_table import HistoricalFeatureTableTaskPayload
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.worker.task.base import BaseTask


class HistoricalFeatureTableTask(BaseTask):
    """
    HistoricalFeatureTableTask creates a HistoricalFeatureTable by computing historical features
    """

    payload_class = HistoricalFeatureTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute HistoricalFeatureTableTask
        """
        payload = cast(HistoricalFeatureTableTaskPayload, self.payload)

        app_container = self.app_container

        observation_table_service: ObservationTableService = app_container.observation_table_service
        observation_table_model = await observation_table_service.get_document(
            payload.observation_table_id
        )

        historical_feature_table_service: HistoricalFeatureTableService = app_container.history_feature_table_service
        location = await historical_feature_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        preview_service: PreviewService = app_container.preview_service

        await preview_service.get_historical_features(
            observation_set=observation_table_model,
            featurelist_get_historical_features=payload.featurelist_get_historical_features,
            get_credential=self.get_credential,
            output_table_details=location.table_details,
        )

        historical_feature_table_model = HistoricalFeatureTableModel(
            _id=payload.output_document_id,
            user_id=self.payload.user_id,
            name=payload.name,
            location=location,
            observation_table_id=payload.observation_table_id,
            feature_list_id=payload.featurelist_get_historical_features.feature_list_id,
        )
        created_doc = await historical_feature_table_service.create_document(historical_feature_table_model)
        assert created_doc.id == payload.output_document_id
