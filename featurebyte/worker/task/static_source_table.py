"""
StaticSourceTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.task.static_source_table import StaticSourceTableTaskPayload
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.static_source_table import StaticSourceTableService
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class StaticSourceTableTask(DataWarehouseMixin, BaseTask):
    """
    StaticSourceTable Task
    """

    payload_class = StaticSourceTableTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        payload: dict[str, Any],
        progress: Any,
        user: User,
        persistent: Persistent,
        storage: Storage,
        temp_storage: Storage,
        feature_store_service: FeatureStoreService,
        static_source_table_service: StaticSourceTableService,
        session_manager_service: SessionManagerService,
    ):
        super().__init__(
            task_id=task_id,
            payload=payload,
            progress=progress,
            user=user,
            persistent=persistent,
            storage=storage,
            temp_storage=temp_storage,
        )
        self.feature_store_service = feature_store_service
        self.static_source_table_service = static_source_table_service
        self.session_manager_service = session_manager_service

    async def get_task_description(self) -> str:
        payload = cast(StaticSourceTableTaskPayload, self.payload)
        return f'Save static source table "{payload.name}"'

    async def execute(self) -> Any:
        """
        Execute StaticSourceTable task
        """
        payload = cast(StaticSourceTableTaskPayload, self.payload)
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

        async with self.drop_table_on_error(db_session, location.table_details, self.payload):
            additional_metadata = (
                await self.static_source_table_service.validate_materialized_table_and_get_metadata(
                    db_session, location.table_details
                )
            )
            logger.debug("Creating a new StaticSourceTable", extra=location.table_details.dict())
            static_source_table = StaticSourceTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                request_input=payload.request_input,
                **additional_metadata,
            )
            await self.static_source_table_service.create_document(static_source_table)
