"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableTask(DataWarehouseMixin, BaseTask):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute ObservationTable task
        """
        payload = cast(ObservationTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)
        location = (
            await self.app_container.observation_table_service.generate_materialized_table_location(
                self.get_credential,
                payload.feature_store_id,
            )
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )

        async with self.drop_table_on_error(db_session, location.table_details):
            additional_metadata = await self.app_container.observation_table_service.validate_materialized_table_and_get_metadata(
                db_session, location.table_details
            )
            logger.debug("Creating a new ObservationTable", extra=location.table_details.dict())
            observation_table = ObservationTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                **additional_metadata,
            )
            await self.app_container.observation_table_service.create_document(observation_table)
