"""
Target API route controller
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from fastapi import UploadFile

from featurebyte.exception import DocumentNotFoundError
from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.observation_table import TargetInput
from featurebyte.models.target_table import TargetTableModel
from featurebyte.routes.common.feature_or_target_table import (
    FeatureOrTargetTableController,
    ValidationParameters,
)
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import TargetTableInfo
from featurebyte.schema.target_table import TargetTableCreate, TargetTableList
from featurebyte.schema.task import Task
from featurebyte.schema.worker.task.target_table import TargetTableTaskPayload
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.target import TargetService
from featurebyte.service.target_namespace import TargetNamespaceService
from featurebyte.service.target_table import TargetTableService


class TargetTableController(
    FeatureOrTargetTableController[
        TargetTableModel,
        TargetTableService,
        TargetTableList,
        TargetTableInfo,
        TargetTableTaskPayload,
        TargetTableCreate,
    ],
):
    """
    TargetTable Controller
    """

    paginated_document_class = TargetTableList
    info_class = TargetTableInfo

    def __init__(
        self,
        target_table_service: TargetTableService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        feature_store_service: FeatureStoreService,
        observation_table_service: ObservationTableService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
        target_service: TargetService,
        target_namespace_service: TargetNamespaceService,
    ):
        super().__init__(
            service=target_table_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
            observation_table_service=observation_table_service,
            entity_validation_service=entity_validation_service,
            task_controller=task_controller,
        )
        self.feature_store_service = feature_store_service
        self.target_service = target_service
        self.target_namespace_service = target_namespace_service

    async def get_payload(
        self, table_create: TargetTableCreate, observation_set_dataframe: Optional[pd.DataFrame]
    ) -> TargetTableTaskPayload:
        return await self.service.get_target_table_task_payload(
            data=table_create, observation_set_dataframe=observation_set_dataframe
        )

    async def get_validation_parameters(
        self, table_create: TargetTableCreate
    ) -> ValidationParameters:
        feature_store = await self.feature_store_service.get_document(
            document_id=table_create.feature_store_id
        )
        # Handle backward compatibility for requests from older SDK
        if isinstance(table_create.request_input, TargetInput):
            target_id = table_create.request_input.target_id
        else:
            target_id = table_create.target_id

        try:
            if target_id is None:
                raise DocumentNotFoundError
            target = await self.target_service.get_document(target_id)
            graph = target.graph
            nodes = [graph.get_node_by_name(target.node.name)]
        except DocumentNotFoundError:
            assert table_create.graph is not None
            graph = table_create.graph
            nodes = table_create.nodes

        assert graph is not None
        return ValidationParameters(
            graph=graph,
            nodes=nodes,
            feature_store=feature_store,
            serving_names_mapping=table_create.serving_names_mapping,
        )

    async def get_additional_info_params(
        self, document: BaseFeatureOrTargetTableModel
    ) -> dict[str, Any]:
        assert isinstance(document, TargetTableModel)
        if document.target_namespace_id is None:
            return {}
        target_namespace = await self.target_namespace_service.get_document(
            document.target_namespace_id
        )
        return {"target_name": target_namespace.name}

    async def create_table(
        self,
        data: TargetTableCreate,
        observation_set: Optional[UploadFile],
    ) -> Task:
        return await super().create_table(data=data, observation_set=observation_set)
