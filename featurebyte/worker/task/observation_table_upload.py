"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from pathlib import Path

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableUploadTask(DataWarehouseMixin, BaseTask):
    """
    ObservationTableUpload Task
    """

    payload_class = ObservationTableUploadTaskPayload

    async def get_task_description(self) -> str:
        payload = cast(ObservationTableTaskPayload, self.payload)
        return f'Upload observation table "{payload.name}" from CSV.'

    async def execute(self) -> Any:
        """
        Execute ObservationTableUpload task
        """
        payload = cast(ObservationTableUploadTaskPayload, self.payload)
        feature_store = await self.app_container.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)
        observation_table_service: ObservationTableService = (
            self.app_container.observation_table_service
        )

        # Retrieve uploaded file from temp storage
        uploaded_dataframe = await self.temp_storage.get_dataframe(
            Path(payload.observation_set_storage_path)
        )

        # Get location for the new observation table
        location = await observation_table_service.generate_materialized_table_location(
            payload.feature_store_id,
        )

        # Write the file to the warehouse
        await db_session.register_table(location.table_details.table_name, uploaded_dataframe)

        async with self.drop_table_on_error(db_session, location.table_details):
            # Validate table and retrieve metadata about the table
            additional_metadata = (
                await observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                )
            )

            # Store metadata of the observation table in mongo
            logger.debug("Creating a new ObservationTable", extra=location.table_details.dict())
            observation_table = ObservationTableModel(
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                request_input=UploadedFileInput(type=RequestInputType.UPLOADED_FILE),
                purpose=payload.purpose,
                **additional_metadata,
            )
            await observation_table_service.create_document(observation_table)
