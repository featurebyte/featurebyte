"""
Target API route controller
"""
from __future__ import annotations

from typing import Any, Optional

from http import HTTPStatus

from bson import ObjectId
from fastapi import HTTPException, UploadFile

from featurebyte.common.utils import dataframe_from_arrow_stream
from featurebyte.models.target_table import TargetTableModel
from featurebyte.routes.common.base_materialized_table import BaseMaterializedTableController
from featurebyte.routes.common.feature_or_target_table import FeatureOrTargetTableController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import TargetTableInfo
from featurebyte.schema.target_table import TargetTableCreate, TargetTableList
from featurebyte.schema.task import Task
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_or_target_helper.info_helper import (
    FeatureOrTargetInfoHelper,
    InfoType,
)
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService
from featurebyte.service.target import TargetService
from featurebyte.service.target_table import TargetTableService


class TargetTableController(
    FeatureOrTargetTableController[TargetTableModel, TargetTableService, TargetTableList],
):
    """
    TargetTable Controller
    """

    paginated_document_class = TargetTableList
    info_type = InfoType.TARGET
    info_class = TargetTableInfo

    def __init__(
        self,
        target_table_service: TargetTableService,
        preview_service: PreviewService,
        feature_store_service: FeatureStoreService,
        observation_table_service: ObservationTableService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
        target_service: TargetService,
        feature_or_target_info_helper: FeatureOrTargetInfoHelper,
    ):
        super().__init__(
            service=target_table_service,
            preview_service=preview_service,
            feature_or_target_info_helper=feature_or_target_info_helper,
        )
        self.feature_store_service = feature_store_service
        self.observation_table_service = observation_table_service
        self.entity_validation_service = entity_validation_service
        self.task_controller = task_controller
        self.target_service = target_service
        self.feature_or_target_info_helper = feature_or_target_info_helper

    async def create_target_table(
        self,
        data: TargetTableCreate,
        observation_set: Optional[UploadFile],
    ) -> Task:
        """
        Create TargetTable by submitting an async historical feature request task

        Parameters
        ----------
        data: TargetTableCreate
            TargetTable creation payload
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

        # feature cluster group feature graph by feature store ID, only single feature store is
        # supported
        feature_store = await self.feature_store_service.get_document(
            document_id=data.feature_store_id
        )
        await self.entity_validation_service.validate_entities_or_prepare_for_parent_serving(
            graph=data.graph,
            nodes=data.nodes,
            request_column_names=request_column_names,
            feature_store=feature_store,
            serving_names_mapping=data.serving_names_mapping,
        )

        # prepare task payload and submit task
        payload = await self.service.get_target_table_task_payload(
            data=data, observation_set_dataframe=observation_set_dataframe
        )
        task_id = await self.task_controller.task_manager.submit(payload=payload)
        return await self.task_controller.get_task(task_id=str(task_id))

    async def get_additional_info_params(self, document_id: ObjectId) -> dict[str, Any]:
        target_table = await self.service.get_document(document_id=document_id)
        target = await self.target_service.get_document(target_table.target_id)
        return {"target_name": target.name}
