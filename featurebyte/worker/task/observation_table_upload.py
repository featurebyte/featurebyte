"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any

from pathlib import Path
from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin

logger = get_logger(__name__)


class ObservationTableUploadTask(DataWarehouseMixin, BaseTask[ObservationTableUploadTaskPayload]):
    """
    ObservationTableUpload Task
    """

    payload_class = ObservationTableUploadTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        task_id: UUID,
        progress: Any,
        temp_storage: Storage,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
    ):
        super().__init__(
            task_id=task_id,
            progress=progress,
        )
        self.temp_storage = temp_storage
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service

    async def get_task_description(self, payload: ObservationTableUploadTaskPayload) -> str:
        return f'Upload observation table "{payload.name}" from CSV.'

    async def execute(self, payload: ObservationTableUploadTaskPayload) -> Any:
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        # Retrieve uploaded file from temp storage
        uploaded_dataframe = await self.temp_storage.get_dataframe(
            Path(payload.observation_set_storage_path)
        )

        # Get location for the new observation table
        location = await self.observation_table_service.generate_materialized_table_location(
            payload.feature_store_id,
        )

        # Write the file to the warehouse
        await db_session.register_table(location.table_details.table_name, uploaded_dataframe)

        async with self.drop_table_on_error(db_session, location.table_details, payload):
            # Validate table and retrieve metadata about the table
            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                )
            )

            # Store metadata of the observation table in mongo
            logger.debug("Creating a new ObservationTable", extra=location.table_details.dict())
            observation_table = ObservationTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                request_input=UploadedFileInput(type=RequestInputType.UPLOADED_FILE),
                purpose=payload.purpose,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
