"""
BatchFeatureTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.worker.task.base import BaseTask


class BatchFeatureTableTask(BaseTask):
    """
    BatchFeatureTableTask creates a batch feature table by computing online predictions
    """

    payload_class = BatchFeatureTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute BatchFeatureTableTask
        """
        payload = cast(BatchFeatureTableTaskPayload, self.payload)

        app_container = self.app_container

        observation_table_service: ObservationTableService = app_container.observation_table_service
        observation_table_model = await observation_table_service.get_document(
            payload.observation_table_id
        )

        batch_feature_table_service: BatchFeatureTableService = (
            app_container.batch_feature_table_service
        )
        location = await batch_feature_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        # TODO: Implement prediction table creation task at warehouse
        _ = observation_table_model

        batch_feature_table_model = BatchFeatureTableModel(
            _id=payload.output_document_id,
            user_id=self.payload.user_id,
            name=payload.name,
            location=location,
            observation_table_id=payload.observation_table_id,
            feature_list_id=payload.feature_list_id,
        )
        created_doc = await batch_feature_table_service.create_document(batch_feature_table_model)
        assert created_doc.id == payload.output_document_id
