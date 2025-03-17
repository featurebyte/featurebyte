"""
ObservationTable API route controller
"""

from __future__ import annotations

from typing import Any, Callable, List, Optional, Tuple

import pandas as pd
from bson import ObjectId
from fastapi import UploadFile

from featurebyte.enum import SpecialColumnName, UploadFileFormat
from featurebyte.exception import UnsupportedObservationTableUploadFileFormat
from featurebyte.logging import get_logger
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import ObservationTableInfo
from featurebyte.schema.observation_table import (
    ObservationTableCreate,
    ObservationTableList,
    ObservationTableModelResponse,
    ObservationTableServiceUpdate,
    ObservationTableUpdate,
    ObservationTableUpload,
)
from featurebyte.schema.task import Task
from featurebyte.service.context import ContextService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.target_table import TargetTableService
from featurebyte.service.task_manager import TaskManager
from featurebyte.service.use_case import UseCaseService

logger = get_logger(__name__)


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
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
        feature_store_service: FeatureStoreService,
        historical_feature_table_service: HistoricalFeatureTableService,
        target_table_service: TargetTableService,
        context_service: ContextService,
        task_manager: TaskManager,
        use_case_service: UseCaseService,
        target_namespace_service: TargetNamespaceService,
    ):
        super().__init__(
            service=observation_table_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
        )
        self.observation_table_service = observation_table_service
        self.task_controller = task_controller
        self.feature_store_service = feature_store_service
        self.historical_feature_table_service = historical_feature_table_service
        self.target_table_service = target_table_service
        self.context_service = context_service
        self.task_manager = task_manager
        self.use_case_service = use_case_service
        self.target_namespace_service = target_namespace_service

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

    async def get_observable_table(self, document_id: ObjectId) -> ObservationTableModelResponse:
        """
        Get ObservationTable

        Parameters
        ----------
        document_id: ObjectId
            ObservationTable document ID

        Returns
        -------
        ObservationTableModelResponse
        """
        observation_table: ObservationTableModel = await self.get(document_id=document_id)
        return ObservationTableModelResponse(
            **observation_table.model_dump(by_alias=True),
            is_valid=observation_table.check_table_is_valid(),
        )

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

        def try_read_as_dataframe(
            read_func: Callable[[str], pd.DataFrame], *args: Any
        ) -> pd.DataFrame:
            try:
                return read_func(*args)
            except Exception as exc:
                raise UnsupportedObservationTableUploadFileFormat(
                    "Content of uploaded file is not valid"
                ) from exc

        filename = observation_set_file.filename.lower()
        if not filename.endswith(".csv") and not filename.endswith(".parquet"):
            raise UnsupportedObservationTableUploadFileFormat(
                "Only csv and parquet file formats are supported for observation set upload"
            )

        if filename.endswith(".csv"):
            file_format = UploadFileFormat.CSV
            observation_set_dataframe = try_read_as_dataframe(
                pd.read_csv, observation_set_file.file
            )
            # Convert point_in_time column to datetime
            if SpecialColumnName.POINT_IN_TIME in observation_set_dataframe.columns:
                observation_set_dataframe[SpecialColumnName.POINT_IN_TIME] = pd.to_datetime(
                    observation_set_dataframe[SpecialColumnName.POINT_IN_TIME]
                )
        else:
            file_format = UploadFileFormat.PARQUET
            observation_set_dataframe = try_read_as_dataframe(
                pd.read_parquet, observation_set_file.file
            )

        payload = await self.service.get_observation_table_upload_task_payload(
            data=data,
            observation_set_dataframe=observation_set_dataframe,
            file_format=file_format,
            uploaded_file_name=observation_set_file.filename,
        )
        task_id = await self.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        document = await self.service.get_document(document_id=document_id)
        return [
            (self.historical_feature_table_service, {"observation_table_id": document_id}),
            (self.target_table_service, {"observation_table_id": document_id}),
            (self.use_case_service, {"_id": {"$in": document.use_case_ids}}),
        ]

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
        if observation_table.target_namespace_id is not None:
            target_namespace = await self.target_namespace_service.get_document(
                document_id=observation_table.target_namespace_id
            )
            target_name = target_namespace.name
        else:
            target_name = None
        return ObservationTableInfo(
            name=observation_table.name,
            type=observation_table.request_input.type,
            feature_store_name=feature_store.name,
            table_details=observation_table.location.table_details,
            target_name=target_name,
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
            observation_table_id, ObservationTableServiceUpdate(**data.model_dump(by_alias=True))
        )
