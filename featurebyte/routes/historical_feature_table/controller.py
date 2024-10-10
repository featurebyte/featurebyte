"""
HistoricalTable API route controller
"""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd
from bson import ObjectId

from featurebyte.models.base_feature_or_target_table import BaseFeatureOrTargetTableModel
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.routes.common.feature_or_target_table import (
    FeatureOrTargetTableController,
    ValidationParameters,
)
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.historical_feature_table import (
    HistoricalFeatureTableCreate,
    HistoricalFeatureTableList,
    HistoricalFeatureTableUpdate,
)
from featurebyte.schema.info import HistoricalFeatureTableInfo
from featurebyte.schema.worker.task.historical_feature_table import (
    HistoricalFeatureTableTaskPayload,
)
from featurebyte.service.entity_validation import EntityValidationService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.historical_feature_table import HistoricalFeatureTableService
from featurebyte.service.historical_features import HistoricalFeaturesValidationParametersService
from featurebyte.service.observation_table import ObservationTableService


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
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        feature_store_service: FeatureStoreService,
        observation_table_service: ObservationTableService,
        entity_validation_service: EntityValidationService,
        task_controller: TaskController,
        feature_list_service: FeatureListService,
        historical_features_validation_parameters_service: HistoricalFeaturesValidationParametersService,
        feature_service: FeatureService,
    ):
        super().__init__(
            service=historical_feature_table_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
            observation_table_service=observation_table_service,
            entity_validation_service=entity_validation_service,
            task_controller=task_controller,
        )
        self.feature_store_service = feature_store_service
        self.feature_list_service = feature_list_service
        self.historical_features_validation_parameters_service = (
            historical_features_validation_parameters_service
        )
        self.feature_service = feature_service

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
        return (
            await self.historical_features_validation_parameters_service.get_validation_parameters(
                table_create.featurelist_get_historical_features
            )
        )

    async def get_additional_info_params(
        self, document: BaseFeatureOrTargetTableModel
    ) -> dict[str, Any]:
        assert isinstance(document, HistoricalFeatureTableModel)
        if document.feature_list_id is None:
            return {}
        feature_list = await self.feature_list_service.get_document(
            document_id=document.feature_list_id
        )
        return {
            "feature_list_name": feature_list.name,
            "feature_list_version": feature_list.version.to_str(),
        }

    async def update_historical_feature_table(
        self, historical_feature_table_id: ObjectId, data: HistoricalFeatureTableUpdate
    ) -> HistoricalFeatureTableModel:
        """
        Update HistoricalFeatureTable

        Parameters
        ----------
        historical_feature_table_id: ObjectId
            HistoricalFeatureTable document_id
        data: HistoricalFeatureTableUpdate
            HistoricalFeatureTable update payload

        Returns
        -------
        Optional[HistoricalFeatureTableModel]
        """
        table = await self.service.update_document(
            historical_feature_table_id, data, return_document=True
        )
        assert isinstance(table, HistoricalFeatureTableModel)
        return table

    async def preview_feature(
        self,
        document_id: ObjectId,
        feature_id: ObjectId,
        limit: int,
    ) -> dict[str, Any]:
        """
        Preview feature from materialized table as pyarrow table

        Parameters
        ----------
        document_id: ObjectId
            ID of materialized table to preview
        feature_id: ObjectId
            ID of feature to preview
        limit: int
            Number of rows to preview

        Returns
        -------
        dict[str, Any]
            Preview of feature from materialized table
        """
        # include only column names from observation table and feature
        table = await self.service.get_document(document_id=document_id)
        assert table.observation_table_id is not None
        observation_table = await self.observation_table_service.get_document(
            table.observation_table_id
        )
        feature = await self.feature_service.get_document(feature_id)
        assert feature.name is not None

        return await self.preview_materialized_table(
            document_id=document_id,
            limit=limit,
            column_names=[col.name for col in observation_table.columns_info] + [feature.name],
        )
