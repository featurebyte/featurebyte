"""
StaticSourceTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.static_source_table import StaticSourceTableModel
from featurebyte.schema.worker.task.static_source_table import StaticSourceTableTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class StaticSourceTableTask(DataWarehouseMixin, BaseTask):
    """
    StaticSourceTable Task
    """

    payload_class = StaticSourceTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute StaticSourceTable task
        """
        payload = cast(StaticSourceTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)
        location = await self.app_container.static_source_table_service.generate_materialized_table_location(
            self.get_credential,
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )

        async with self.drop_table_on_error(db_session, location.table_details):
            additional_metadata = await self.app_container.static_source_table_service.validate_materialized_table_and_get_metadata(
                db_session, location.table_details
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
            await self.app_container.static_source_table_service.create_document(
                static_source_table
            )
