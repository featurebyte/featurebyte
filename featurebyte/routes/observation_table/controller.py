"""
ObservationTable API route controller
"""
from __future__ import annotations

from typing import Optional

import pandas as pd
from bson import ObjectId
from fastapi import UploadFile

from featurebyte.enum import SpecialColumnName
from featurebyte.exception import UnsupportedObservationTableUploadFileFormat
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import ObservationTableInfo
from featurebyte.schema.observation_table import (
    ObservationTableCreate,
    ObservationTableList,
    ObservationTableUpdate,
    ObservationTableUpload,
)
from featurebyte.schema.task import Task
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.use_case import UseCaseService
from featurebyte.service.validator.materialized_table_delete import ObservationTableDeleteValidator


class ObservationTableController(
    BaseMaterializedTableController[
        ObservationTableModel, ObservationTableService, ObservationTableList
    ],
):
    """
    ObservationTable Controller
    """

    paginated_document_class = ObservationTableList

    def __init__(
        self,
        observation_table_service: ObservationTableService,
        preview_service: PreviewService,
        task_controller: TaskController,
        feature_store_service: FeatureStoreService,
        observation_table_delete_validator: ObservationTableDeleteValidator,
        context_service: ContextService,
        task_manager: TaskManager,
        use_case_service: UseCaseService,
    ):
        super().__init__(service=observation_table_service, preview_service=preview_service)
        self.observation_table_service = observation_table_service
        self.task_controller = task_controller
        self.feature_store_service = feature_store_service
        self.observation_table_delete_validator = observation_table_delete_validator
        self.context_service = context_service
        self.task_manager = task_manager
        self.use_case_service = use_case_service

    async def create_observation_table(
        self,
        data: ObservationTableCreate,
    ) -> Task:
        """
        Create ObservationTable by submitting a materialization task

        Parameters
        ----------
        data: ObservationTableCreate
            ObservationTable creation payload

        Returns
        -------
        Task
        """
        payload = await self.service.get_observation_table_task_payload(data=data)
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def upload_observation_table(
        self, data: ObservationTableUpload, observation_set_file: UploadFile
    ) -> Task:
        """
        Create ObservationTable by submitting a materialization task

        Parameters
        ----------
        data: ObservationTableUpload
            ObservationTableUpload creation payload
        observation_set_file: UploadFile
            Observation set file

        Returns
        -------
        Task

        Raises
        ------
        UnsupportedObservationTableUploadFileFormat
            if the observation set file format is not supported
        """
        assert observation_set_file.filename is not None
        if observation_set_file.filename.lower().endswith(".csv"):
            assert observation_set_file.content_type == "text/csv"
            observation_set_dataframe = pd.read_csv(observation_set_file.file)
            # Convert point_in_time column to datetime
            observation_set_dataframe[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
                observation_set_dataframe[SpecialColumnName.POINT_IN_TIME]
            )
        elif observation_set_file.filename.lower().endswith(".parquet"):
            assert observation_set_file.content_type == "application/octet-stream"
            observation_set_dataframe = pd.read_parquet(observation_set_file.file)
        else:
            raise UnsupportedObservationTableUploadFileFormat(
                "Only csv and parquet file formats are supported for observation set upload"
            )
        payload = await self.service.get_observation_table_upload_task_payload(
            data=data, observation_set_dataframe=observation_set_dataframe
        )
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def _verify_delete_operation(self, document_id: ObjectId) -> None:
        await self.observation_table_delete_validator.check_delete_observation_table(
            observation_table_id=document_id,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ObservationTableInfo:
        """
        Get ObservationTable info given document_id

        Parameters
        ----------
        document_id: ObjectId
            ObservationTable document_id
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        ObservationTableInfo
        """
        _ = verbose
        observation_table = await self.service.get_document(document_id=document_id)
        feature_store = await self.feature_store_service.get_document(
            document_id=observation_table.location.feature_store_id
        )
        return ObservationTableInfo(
            name=observation_table.name,
            type=observation_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=observation_table.location.table_details,
            created_at=observation_table.created_at,
            updated_at=observation_table.updated_at,
            description=observation_table.description,
        )

    async def update_observation_table(
        self, observation_table_id: ObjectId, data: ObservationTableUpdate
    ) -> Optional[ObservationTableModel]:
        """
        Update ObservationTable

        Parameters
        ----------
        observation_table_id: ObjectId
            ObservationTable document_id
        data: ObservationTableUpdate
            ObservationTable update payload

        Returns
        -------
        Optional[ObservationTableModel]
        """

        return await self.observation_table_service.update_observation_table(
            observation_table_id, data
        )
