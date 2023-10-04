"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, TargetInput
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableTask(DataWarehouseMixin, BaseTask):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    async def get_task_description(self) -> str:
        payload = cast(ObservationTableTaskPayload, self.payload)
        return f'Save observation table "{payload.name}"'

    async def execute(self) -> Any:
        """
        Execute ObservationTable task
        """
        payload = cast(ObservationTableTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)
        observation_table_service: ObservationTableService = (
            self.app_container.observation_table_service
        )
        location = await observation_table_service.generate_materialized_table_location(
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )

        async with self.drop_table_on_error(db_session, location.table_details):
            payload_input = payload.request_input
            assert not isinstance(payload_input, TargetInput)
            additional_metadata = (
                await observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    skip_entity_validation_checks=payload.skip_entity_validation_checks,
                )
            )
            logger.debug("Creating a new ObservationTable", extra=location.table_details.dict())
            observation_table = ObservationTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                purpose=payload.purpose,
                **additional_metadata,
            )
            await observation_table_service.create_document(observation_table)
