"""
BatchRequestTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logger import logger
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.query_graph.node.schema import ColumnSpec
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.session.manager import SessionManager
from featurebyte.worker.task.base import BaseTask


class BatchRequestTableTask(BaseTask):
    """
    BatchRequestTable Task
    """

    payload_class = BatchRequestTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute BatchRequestTable task

        Raises
        ------
        Exception
            If the materialized table fails to be created.
        """
        payload = cast(BatchRequestTableTaskPayload, self.payload)
        persistent = self.get_persistent()

        feature_store_service = FeatureStoreService(
            user=self.user, persistent=persistent, catalog_id=self.payload.catalog_id
        )
        feature_store = await feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        session_manager = SessionManager(
            credentials={
                feature_store.name: await self.get_credential(
                    user_id=payload.user_id, feature_store_name=feature_store.name
                )
            }
        )
        db_session = await session_manager.get_session(feature_store)

        context_service = ContextService(
            user=self.user, persistent=persistent, catalog_id=self.payload.catalog_id
        )
        batch_request_table_service = BatchRequestTableService(
            user=self.user,
            persistent=persistent,
            catalog_id=self.payload.catalog_id,
            context_service=context_service,
            feature_store_service=feature_store_service,
        )
        location = await batch_request_table_service.generate_materialized_table_location(
            self.get_credential,
            payload.feature_store_id,
        )

        try:
            await payload.request_input.materialize(
                session=db_session,
                destination=location.table_details,
                sample_rows=payload.sample_rows,
            )
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
            await batch_request_table_service.create_document(batch_request_table)
        except Exception as exc:
            logger.error(
                "Failed to create BatchRequestTable",
                extras={"error": str(exc), "task_payload": self.payload.dict()},
            )
            assert location.table_details.schema_name is not None
            assert location.table_details.database_name is not None
            await db_session.drop_table(
                table_name=location.table_details.table_name,
                schema_name=location.table_details.schema_name,
                database_name=location.table_details.database_name,
            )
            raise exc
