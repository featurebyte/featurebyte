"""
ForecastTable API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.forecast_table import ForecastTableList, ForecastTableServiceUpdate
from featurebyte.schema.info import ForecastTableInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.forecast_table import ForecastTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.target import TargetService


class ForecastTableController(
    BaseTableDocumentController[ForecastTableModel, ForecastTableService, ForecastTableList]
):
    """
    ForecastTable controller
    """

    paginated_document_class = ForecastTableList
    document_update_schema_class = ForecastTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "natural_key_column": SemanticType.FORECAST_NATURAL_KEY_ID,
        "effective_timestamp_column": SemanticType.FORECAST_EFFECTIVE_TIMESTAMP,
        "forecast_timestamp_column": SemanticType.FORECAST_TIMESTAMP,
    }

    def __init__(
        self,
        forecast_table_service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        entity_service: EntityService,
        feature_service: FeatureService,
        target_service: TargetService,
        feature_list_service: FeatureListService,
        table_info_service: TableInfoService,
        specialized_dtype_detection_service: SpecializedDtypeDetectionService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=forecast_table_service,
            table_facade_service=table_facade_service,
            semantic_service=semantic_service,
            entity_service=entity_service,
            feature_service=feature_service,
            target_service=target_service,
            feature_list_service=feature_list_service,
            specialized_dtype_detection_service=specialized_dtype_detection_service,
            feature_store_service=feature_store_service,
            feature_store_warehouse_service=feature_store_warehouse_service,
            task_controller=task_controller,
        )
        self.table_info_service = table_info_service

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ForecastTableInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        ForecastTableInfo
        """
        forecast_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=forecast_table, verbose=verbose
        )
        return ForecastTableInfo(
            **table_dict,
            natural_key_column=forecast_table.natural_key_column,
            effective_timestamp_column=forecast_table.effective_timestamp_column,
            forecast_timestamp_column=forecast_table.forecast_timestamp_column,
        )
