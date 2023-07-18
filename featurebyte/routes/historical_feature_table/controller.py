"""
HistoricalTable API route controller
"""
from __future__ import annotations

from typing import Any, Optional

import pandas as pd

from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.routes.common.feature_or_target_table import (
    FeatureOrTargetTableController,
    ValidationParameters,
)
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.historical_feature_table import (
    HistoricalFeatureTableCreate,
    HistoricalFeatureTableList,
)
from featurebyte.schema.info import HistoricalFeatureTableInfo
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.observation_table import ObservationTableService
from featurebyte.service.preview import PreviewService


class HistoricalFeatureTableController(
    FeatureOrTargetTableController[
        HistoricalFeatureTableModel,
        HistoricalFeatureTableService,
        HistoricalFeatureTableList,
        HistoricalFeatureTableInfo,
        HistoricalFeatureTableTaskPayload,
        HistoricalFeatureTableCreate,
    ],
):
    """
    HistoricalFeatureTable Controller
    """

    paginated_document_class = HistoricalFeatureTableList
    info_class = HistoricalFeatureTableInfo

    def __init__(
        self,
        historical_feature_table_service: HistoricalFeatureTableService,
        preview_service: PreviewService,
        feature_store_service: FeatureStoreService,
        observation_table_service: ObservationTableService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
        feature_list_service: FeatureListService,
    ):
        super().__init__(
            service=historical_feature_table_service,
            preview_service=preview_service,
            observation_table_service=observation_table_service,
            entity_validation_service=entity_validation_service,
            task_controller=task_controller,
        )
        self.feature_store_service = feature_store_service
        self.feature_list_service = feature_list_service

    async def get_payload(
        self,
        table_create: HistoricalFeatureTableCreate,
        observation_set_dataframe: Optional[pd.DataFrame],
    ) -> HistoricalFeatureTableTaskPayload:
        return await self.service.get_historical_feature_table_task_payload(
            data=table_create, observation_set_dataframe=observation_set_dataframe
        )

    async def get_validation_parameters(
        self, table_create: HistoricalFeatureTableCreate
    ) -> ValidationParameters:
        # feature cluster group feature graph by feature store ID, only single feature store is
        # supported
        feature_cluster = table_create.featurelist_get_historical_features.feature_clusters[0]
        feature_store = await self.feature_store_service.get_document(
            document_id=feature_cluster.feature_store_id
        )
        return ValidationParameters(
            graph=feature_cluster.graph,
            nodes=feature_cluster.nodes,
            feature_store=feature_store,
            serving_names_mapping=table_create.featurelist_get_historical_features.serving_names_mapping,
        )

    async def get_additional_info_params(
        self, document: HistoricalFeatureTableModel
    ) -> dict[str, Any]:
        feature_list = await self.feature_list_service.get_document(
            document_id=document.feature_list_id
        )
        return {
            "feature_list_name": feature_list.name,
            "feature_list_version": feature_list.version.to_str(),
        }
