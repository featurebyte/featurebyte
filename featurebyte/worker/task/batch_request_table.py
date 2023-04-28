"""
BatchRequestTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class BatchRequestTableTask(DataWarehouseMixin, BaseTask):
    """
    BatchRequestTable Task
    """

    payload_class = BatchRequestTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute BatchRequestTable task
        """
        payload = cast(BatchRequestTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)
        service = self.app_container.batch_request_table_service
        location = await service.generate_materialized_table_location(
            self.get_credential,
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=None,
        )

        async with self.drop_table_on_error(db_session, location.table_details):
            columns_info, num_rows = await service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug("Creating a new BatchRequestTable", extra=location.table_details.dict())
            batch_request_table = BatchRequestTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                columns_info=columns_info,
                num_rows=num_rows,
            )
            await self.app_container.batch_request_table_service.create_document(
                batch_request_table
            )
