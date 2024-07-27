"""
Feature or target table controller
"""

from __future__ import annotations

from abc import abstractmethod
from dataclasses import dataclass
from http import HTTPStatus
from typing import Any, Generic, List, Optional, TypeVar

import pandas as pd
from bson import ObjectId
from fastapi import HTTPException, UploadFile

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.feature_list import FeatureListModel
from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.models.target_table import TargetTableModel
from featurebyte.query_graph.graph import QueryGraph
from featurebyte.query_graph.node import Node
from featurebyte.routes.common.base import PaginatedDocument
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.common.feature_or_target import FeatureOrTargetTableCreate
from featurebyte.schema.info import (
    BaseFeatureOrTargetTableInfo,
    HistoricalFeatureTableInfo,
    TargetTableInfo,
)
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target_table import TargetTableService

InfoTypeT = TypeVar("InfoTypeT", HistoricalFeatureTableInfo, TargetTableInfo)

MaterializedTableDocumentT = TypeVar(
    "MaterializedTableDocumentT",
    HistoricalFeatureTableModel,
    TargetTableModel,
)
MaterializedTableDocumentServiceT = TypeVar(
    "MaterializedTableDocumentServiceT",
    HistoricalFeatureTableService,
    TargetTableService,
)
PayloadT = TypeVar(
    "PayloadT",
    HistoricalFeatureTableTaskPayload,
    TargetTableTaskPayload,
)
TableCreateT = TypeVar(
    "TableCreateT",
    bound=FeatureOrTargetTableCreate,
)


@dataclass
class ValidationParameters:
    """
    Validation parameters
    """

    graph: QueryGraph
    nodes: List[Node]
    feature_store: FeatureStoreModel
    feature_list_model: Optional[FeatureListModel] = None
    serving_names_mapping: Optional[dict[str, str]] = None


class FeatureOrTargetTableController(
    BaseMaterializedTableController[
        MaterializedTableDocumentT, MaterializedTableDocumentServiceT, PaginatedDocument
    ],
    Generic[
        MaterializedTableDocumentT,
        MaterializedTableDocumentServiceT,
        PaginatedDocument,
        InfoTypeT,
        PayloadT,
        TableCreateT,
    ],
):
    """
    Feature or target table controller
    """

    info_class: type[InfoTypeT]

    def __init__(
        self,
        service: MaterializedTableDocumentServiceT,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        observation_table_service: ObservationTableService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=service, feature_store_warehouse_service=feature_store_warehouse_service
        )
        self.observation_table_service = observation_table_service
        self.entity_validation_service = entity_validation_service
        self.task_controller = task_controller

    @abstractmethod
    async def get_additional_info_params(
        self, document: BaseFeatureOrTargetTableModel
    ) -> dict[str, Any]:
        """
        Get additional info params

        Parameters
        ----------
        document: BaseFeatureOrTargetTableModel
            document

        Returns
        -------
        dict[str, Any]
        """

    @abstractmethod
    async def get_payload(
        self, table_create: TableCreateT, observation_set_dataframe: Optional[pd.DataFrame]
    ) -> PayloadT:
        """
        Get payload

        Parameters
        ----------
        table_create: TableCreateT
            table creation payload
        observation_set_dataframe: Optional[pd.DataFrame]
            observation set dataframe

        Returns
        -------
        PayloadT
        """

    @abstractmethod
    async def get_validation_parameters(self, table_create: TableCreateT) -> ValidationParameters:
        """
        Get validation parameters

        Parameters
        ----------
        table_create: TableCreateT
            table creation payload

        Returns
        -------
        ValidationParameters
        """

    async def create_table(
        self,
        data: TableCreateT,
        observation_set: Optional[UploadFile],
    ) -> Task:
        """
        Create table by submitting an async request task

        Parameters
        ----------
        data: TableCreateT
            table creation payload
        observation_set: Optional[UploadFile]
            Observation set file

        Returns
        -------
        Task

        Raises
        ------
        HTTPException
            If both observation_set and observation_table_id are set
        """
        if data.observation_table_id is not None and observation_set is not None:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail="Only one of observation_set file and observation_table_id can be set",
            )

        # Validate the observation_table_id
        if data.observation_table_id is not None:
            observation_table = await self.observation_table_service.get_document(
                document_id=data.observation_table_id
            )
            observation_set_dataframe = None
            request_column_names = {col.name for col in observation_table.columns_info}
        else:
            assert observation_set is not None
            observation_set_dataframe = dataframe_from_arrow_stream(observation_set.file)
            request_column_names = set(observation_set_dataframe.columns)

        validation_parameters = await self.get_validation_parameters(data)
        await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            graph_nodes=(validation_parameters.graph, validation_parameters.nodes),
            feature_list_model=validation_parameters.feature_list_model,
            request_column_names=request_column_names,
            feature_store=validation_parameters.feature_store,
            serving_names_mapping=validation_parameters.serving_names_mapping,
        )

        # prepare task payload and submit task
        payload = await self.get_payload(
            table_create=data, observation_set_dataframe=observation_set_dataframe
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_basic_info(
        self, document: BaseFeatureOrTargetTableModel
    ) -> BaseFeatureOrTargetTableInfo:
        """
        Get common table info

        Parameters
        ----------
        document: BaseFeatureOrTargetTableModel

        Returns
        -------
        BaseFeatureOrTargetTableInfo

        Raises
        ------
        ValueError
        """
        observation_table: Optional[ObservationTableModel] = None
        if document.observation_table_id is not None:
            observation_table = await self.observation_table_service.get_document(
                document_id=document.observation_table_id
            )
        return BaseFeatureOrTargetTableInfo(
            name=document.name,
            observation_table_name=observation_table.name if observation_table else None,
            table_details=document.location.table_details,
            created_at=document.created_at,
            updated_at=document.updated_at,
            description=document.description,
        )

    async def get_info(self, document_id: ObjectId, verbose: bool) -> InfoTypeT:
        """
        Get info

        Parameters
        ----------
        document_id: ObjectId
            document ID
        verbose: bool
            Whether to return verbose info

        Returns
        -------
        InfoTypeT
        """
        _ = verbose
        document = await self.service.get_document(document_id)
        basic_info = await self.get_basic_info(document)
        additional_params = await self.get_additional_info_params(document)
        return self.info_class(
            **additional_params,
            **basic_info.model_dump(),
        )
