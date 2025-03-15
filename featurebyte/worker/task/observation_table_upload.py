"""
ObservationTable creation task
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np

from featurebyte.enum import InternalName
from featurebyte.logging import get_logger
from featurebyte.models.observation_table import SourceTableObservationInput, UploadedFileInput
from featurebyte.models.request_input import RequestInputType
from featurebyte.query_graph.model.common_table import TabularSource
from featurebyte.schema.observation_table import ObservationTableCreate
from featurebyte.schema.worker.task.observation_table_upload import (
    ObservationTableUploadTaskPayload,
)
from featurebyte.service.catalog import CatalogService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.session_manager import SessionManagerService
from featurebyte.service.task_manager import TaskManager
from featurebyte.storage import Storage
from featurebyte.worker.task.base import BaseTask
from featurebyte.worker.task.mixin import DataWarehouseMixin
from featurebyte.worker.task.observation_table import ObservationTableTask
from featurebyte.worker.util.task_progress_updater import TaskProgressUpdater

logger = get_logger(__name__)


class ObservationTableUploadTask(DataWarehouseMixin, BaseTask[ObservationTableUploadTaskPayload]):
    """
    ObservationTableUpload Task
    """

    payload_class = ObservationTableUploadTaskPayload

    def __init__(
        self,
        task_manager: TaskManager,
        temp_storage: Storage,
        feature_store_service: FeatureStoreService,
        session_manager_service: SessionManagerService,
        observation_table_service: ObservationTableService,
        observation_table_task: ObservationTableTask,
        catalog_service: CatalogService,
        task_progress_updater: TaskProgressUpdater,
    ):
        super().__init__(task_manager=task_manager)
        self.temp_storage = temp_storage
        self.feature_store_service = feature_store_service
        self.session_manager_service = session_manager_service
        self.observation_table_service = observation_table_service
        self.observation_table_task = observation_table_task
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

        # Write the file to the warehouse. Add row index to the DataFrame in memory to ensure that
        # the row index follows the same order as the uploaded file.
        uploaded_dataframe.insert(
            0, InternalName.TABLE_ROW_INDEX.value, np.arange(1, len(uploaded_dataframe) + 1)
        )
        await db_session.register_table(location.table_details.table_name, uploaded_dataframe)

        async with self.drop_table_on_error(
            db_session=db_session,
            list_of_table_details=[location.table_details],
            payload=payload,
        ):
            # trigger observation table creation
            obs_task_payload = (
                await self.observation_table_service.get_observation_table_task_payload(
                    data=ObservationTableCreate(
                        _id=payload.id,
                        name=payload.name,
                        feature_store_id=feature_store_id,
                        request_input=SourceTableObservationInput(
                            source=TabularSource(
                                feature_store_id=feature_store_id,
                                table_details=location.table_details,
                            )
                        ),
                        purpose=payload.purpose,
                        primary_entity_ids=payload.primary_entity_ids,
                        target_column=payload.target_column,
                    ),
                    # set to_add_row_index to False since the row index is already added
                    to_add_row_index=False,
                )
            )

            # create observation table by calling the observation table task
            await self.observation_table_task.create_observation_table(
                payload=obs_task_payload,
                override_model_params={
                    "request_input": UploadedFileInput(
                        type=RequestInputType.UPLOADED_FILE,
                        file_name=payload.uploaded_file_name,
                    ),
                    "target_namespace_id": payload.target_namespace_id,
                },
            )

        # drop the temp table
        await db_session.drop_table(
            table_name=location.table_details.table_name,
            schema_name=location.table_details.schema_name,  # type: ignore
            database_name=location.table_details.database_name,  # type: ignore
            if_exists=True,
        )
