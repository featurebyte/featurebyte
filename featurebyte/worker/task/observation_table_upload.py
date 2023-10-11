"""
ObservationTable creation task
"""
from __future__ import annotations

from typing import Any, cast

from pathlib import Path
from uuid import UUID

from featurebyte.logging import get_logger
from featurebyte.models.base import User
from featurebyte.models.observation_table import ObservationTableModel, UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.persistent import Persistent
from featurebyte.schema.worker.task.observation_table import ObservationTableTaskPayload
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


class ObservationTableUploadTask(DataWarehouseMixin, BaseTask):
    """
    ObservationTableUpload Task
    """

    payload_class = ObservationTableUploadTaskPayload

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
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
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
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service

    async def get_task_description(self) -> str:
        payload = cast(ObservationTableTaskPayload, self.payload)
        return f'Upload observation table "{payload.name}" from CSV.'

    async def execute(self) -> Any:
        """
        Execute ObservationTableUpload task
        """
        payload = cast(ObservationTableUploadTaskPayload, self.payload)
        feature_store = await self.feature_store_service.get_document(
            document_id=payload.feature_store_id
        )
        db_session = await self.get_db_session(feature_store)

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

        async with self.drop_table_on_error(db_session, location.table_details):
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
                _id=self.payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                request_input=UploadedFileInput(type=RequestInputType.UPLOADED_FILE),
                purpose=payload.purpose,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
