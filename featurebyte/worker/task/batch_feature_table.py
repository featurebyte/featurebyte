"""
BatchFeatureTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.models.batch_feature_table import BatchFeatureTableModel
from featurebyte.schema.worker.task.batch_feature_table import BatchFeatureTableTaskPayload
from featurebyte.service.batch_feature_table import BatchFeatureTableService
from featurebyte.service.batch_request_table import BatchRequestTableService
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

        batch_request_table_service: BatchRequestTableService = (
            app_container.batch_request_table_service
        )
        batch_request_table_model = await batch_request_table_service.get_document(
            payload.batch_request_table_id
        )

        batch_feature_table_service: BatchFeatureTableService = (
            app_container.batch_feature_table_service
        )
        location = await batch_feature_table_service.generate_materialized_table_location(
            self.get_credential, payload.feature_store_id
        )

        # TODO: Implement batch feature table creation at warehouse
        _ = batch_request_table_model

        batch_feature_table_model = BatchFeatureTableModel(
            _id=payload.output_document_id,
            user_id=self.payload.user_id,
            name=payload.name,
            location=location,
            batch_request_table_id=payload.batch_request_table_id,
            feature_list_id=payload.feature_list_id,
        )
        created_doc = await batch_feature_table_service.create_document(batch_feature_table_model)
        assert created_doc.id == payload.output_document_id
