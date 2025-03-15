"""
BatchRequestTable creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.schema.worker.task.batch_request_table import BatchRequestTableTaskPayload
from featurebyte.service.batch_request_table import BatchRequestTableService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.session.base import BaseSession
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class BatchRequestTableTask(DataWarehouseMixin, BaseTask[BatchRequestTableTaskPayload]):
    """
    BatchRequestTable Task
    """

    payload_class = BatchRequestTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        batch_request_table_service: BatchRequestTableService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.batch_request_table_service = batch_request_table_service

    async def get_task_description(self, payload: BatchRequestTableTaskPayload) -> str:
        return f'Save batch request table "{payload.name}"'

    async def create_batch_request_table(
        self,
        db_session: BaseSession,
        payload: BatchRequestTableTaskPayload,
        create_document: bool = True,
    ) -> BatchRequestTableModel:
        """
        Create batch request table

        Parameters
        ----------
        db_session: BaseSession
            Database session
        payload: BatchRequestTableTaskPayload
            Task payload
        create_document: bool
            Create batch request table document, by default True

        Returns
        -------
        BatchRequestTableModel
            Batch request table model
        """
        service = self.batch_request_table_service
        location = await service.generate_materialized_table_location(
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=None,
        )
        await service.add_row_index_column(db_session, location.table_details)

        async with self.drop_table_on_error(
            db_session=db_session, list_of_table_details=[location.table_details], payload=payload
        ):
            columns_info, num_rows = await service.get_columns_info_and_num_rows(
                db_session, location.table_details
            )
            logger.debug(
                "Creating a new BatchRequestTable", extra=location.table_details.model_dump()
            )
            batch_request_table = BatchRequestTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                columns_info=columns_info,
                num_rows=num_rows,
            )
            if create_document:
                await self.batch_request_table_service.create_document(batch_request_table)
            return batch_request_table

    async def execute(self, payload: BatchRequestTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        await self.create_batch_request_table(db_session, payload)
