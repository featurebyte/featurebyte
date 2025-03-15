"""
StaticSourceTable creation task
"""

from __future__ import annotations

from typing import Any

from featurebyte.logging import get_logger
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.schema.worker.task.static_source_table import StaticSourceTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class StaticSourceTableTask(DataWarehouseMixin, BaseTask[StaticSourceTableTaskPayload]):
    """
    StaticSourceTable Task
    """

    payload_class = StaticSourceTableTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        feature_store_service: FeatureStoreService,
        static_source_table_service: StaticSourceTableService,
        session_manager_service: SessionManagerService,
    ):
        super().__init__(task_manager=task_manager)
        self.feature_store_service = feature_store_service
        self.static_source_table_service = static_source_table_service
        self.session_manager_service = session_manager_service

    async def get_task_description(self, payload: StaticSourceTableTaskPayload) -> str:
        return f'Save static source table "{payload.name}"'

    async def execute(self, payload: StaticSourceTableTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)
        location = await self.static_source_table_service.generate_materialized_table_location(
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )

        async with self.drop_table_on_error(
            db_session=db_session,
            list_of_table_details=[location.table_details],
            payload=payload,
        ):
            additional_metadata = (
                await self.static_source_table_service.validate_materialized_table_and_get_metadata(
                    db_session, location.table_details
                )
            )
            logger.debug(
                "Creating a new StaticSourceTable", extra=location.table_details.model_dump()
            )
            static_source_table = StaticSourceTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                request_input=payload.request_input,
                **additional_metadata,
            )
            await self.static_source_table_service.create_document(static_source_table)
