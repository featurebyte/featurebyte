"""
PredictionTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.prediction_table import PredictionTableModel
from featurebyte.schema.worker.task.prediction_table import PredictionTableTaskPayload
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.prediction_table import PredictionTableService
from featurebyte.worker.task.base import BaseTask


class PredictionTableTask(BaseTask):
    """
    PredictionTableTask creates a prediction table by computing online predictions
    """

    payload_class = PredictionTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute PredictionTableTask
        """
        payload = cast(PredictionTableTaskPayload, self.payload)

        app_container = self.app_container

        observation_table_service: ObservationTableService = app_container.observation_table_service
        observation_table_model = await observation_table_service.get_document(
            payload.observation_table_id
        )

        prediction_table_service: PredictionTableService = app_container.prediction_table_service
        location = await prediction_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        # TODO: Implement prediction table creation task at warehouse
        _ = observation_table_model

        prediction_table_model = PredictionTableModel(
            _id=payload.output_document_id,
            user_id=self.payload.user_id,
            name=payload.name,
            location=location,
            observation_table_id=payload.observation_table_id,
            feature_list_id=payload.feature_list_id,
        )
        created_doc = await prediction_table_service.create_document(prediction_table_model)
        assert created_doc.id == payload.output_document_id
