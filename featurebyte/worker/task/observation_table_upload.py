"""
ObservationTable creation task
"""

from __future__ import annotations

from typing import Any

from pathlib import Path

from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel, UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class ObservationTableUploadTask(DataWarehouseMixin, BaseTask[ObservationTableUploadTaskPayload]):
    """
    ObservationTableUpload Task
    """

    payload_class = ObservationTableUploadTaskPayload

    def __init__(  # pylint: disable=too-many-arguments
        self,
        temp_storage: Storage,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
        catalog_service: CatalogService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__()
        self.temp_storage = temp_storage
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service
        self.catalog_service = catalog_service
        self.task_progress_updater = task_progress_updater

    async def get_task_description(self, payload: ObservationTableUploadTaskPayload) -> str:
        return f'Save observation table "{payload.name}" from {payload.file_format} file.'

    async def execute(self, payload: ObservationTableUploadTaskPayload) -> Any:
        catalog = await self.catalog_service.get_document(document_id=payload.catalog_id)
        feature_store_id = catalog.default_feature_store_ids[0]
        feature_store = await self.feature_store_service.get_document(document_id=feature_store_id)
        db_session = await self.session_manager_service.get_feature_store_session(feature_store)

        # Retrieve uploaded file from temp storage
        uploaded_dataframe = await self.temp_storage.get_dataframe(
            Path(payload.observation_set_storage_path)
        )

        await self.task_progress_updater.update_progress(
            percent=20, message="Creating observation table"
        )

        # Get location for the new observation table
        location = await self.observation_table_service.generate_materialized_table_location(
            feature_store_id
        )

        # Write the file to the warehouse
        await db_session.register_table(location.table_details.table_name, uploaded_dataframe)
        await self.observation_table_service.add_row_index_column(
            db_session, location.table_details
        )

        async with self.drop_table_on_error(db_session, location.table_details, payload):
            # Validate table and retrieve metadata about the table
            additional_metadata = (
                await self.observation_table_service.validate_materialized_table_and_get_metadata(
                    db_session,
                    location.table_details,
                    feature_store=feature_store,
                    primary_entity_ids=payload.primary_entity_ids,
                    target_namespace_id=payload.target_namespace_id,
                )
            )

            # Store metadata of the observation table in mongo
            logger.debug("Creating a new ObservationTable", extra=location.table_details.dict())
            observation_table = ObservationTableModel(
                _id=payload.output_document_id,
                user_id=payload.user_id,
                name=payload.name,
                location=location,
                request_input=UploadedFileInput(
                    type=RequestInputType.UPLOADED_FILE,
                    file_name=payload.uploaded_file_name,
                ),
                purpose=payload.purpose,
                primary_entity_ids=payload.primary_entity_ids,
                has_row_index=True,
                target_namespace_id=payload.target_namespace_id,
                **additional_metadata,
            )
            await self.observation_table_service.create_document(observation_table)
