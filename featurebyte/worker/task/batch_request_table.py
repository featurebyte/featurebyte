"""
BatchRequestTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logger import logger
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin


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
        location = await self.app_container.batch_request_table_service.generate_materialized_table_location(
            self.get_credential,
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )

        async with self.drop_table_on_error(db_session, location.table_details):
            table_schema = await db_session.list_table_schema(
                table_name=location.table_details.table_name,
                database_name=location.table_details.database_name,
                schema_name=location.table_details.schema_name,
            )
            logger.debug("Creating a new BatchRequestTable", extras=location.table_details.dict())
            batch_request_table = BatchRequestTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                columns_info=[
                    ColumnSpec(name=name, dtype=var_type) for name, var_type in table_schema.items()
                ],
            )
            await self.app_container.batch_request_table_service.create_document(
                batch_request_table
            )
