"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from featurebyte.logger import logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import RequestTableMaterializationMixin


class ObservationTableTask(BaseTask, RequestTableMaterializationMixin):
    """
    ObservationTable Task
    """

    payload_class = ObservationTableTaskPayload

    async def execute(self) -> Any:
        """
        Execute ObservationTable task

        Raises
        ------
        Exception
            If the validation on the materialized table fails.
        """
        payload = cast(ObservationTableTaskPayload, self.payload)
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)

        observation_table_service = ObservationTableService(
            user=self.user,
            persistent=self.get_persistent(),
            catalog_id=self.payload.catalog_id,
            context_service=self.context_service,
            feature_store_service=self.feature_store_service,
        )
        location = await observation_table_service.generate_materialized_table_location(
            self.get_credential,
            payload.feature_store_id,
        )
        await payload.request_input.materialize(
            session=db_session,
            destination=location.table_details,
            sample_rows=payload.sample_rows,
        )

        async with self.drop_table_on_error(db_session, location.table_details):
            additional_metadata = (
                await observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session, location.table_details
                )
            )
            logger.debug("Creating a new ObservationTable", extras=location.table_details.dict())
            observation_table = ObservationTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                context_id=payload.context_id,
                request_input=payload.request_input,
                **additional_metadata,
            )
            await observation_table_service.create_document(observation_table)
