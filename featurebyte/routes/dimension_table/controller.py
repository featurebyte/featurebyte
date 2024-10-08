"""
DimensionTable API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.dimension_table import DimensionTableList, DimensionTableServiceUpdate
from featurebyte.schema.info import DimensionTableInfo
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.target import TargetService


class DimensionTableController(
    BaseTableDocumentController[DimensionTableModel, DimensionTableService, DimensionTableList]
):
    """
    DimensionTable controller
    """

    paginated_document_class = DimensionTableList
    document_update_schema_class = DimensionTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "dimension_id_column": SemanticType.DIMENSION_ID,
    }

    def __init__(
        self,
        dimension_table_service: TableDocumentService,
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
            service=dimension_table_service,
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

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DimensionTableInfo:
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
        DimensionTableInfo
        """
        dimension_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=dimension_table, verbose=verbose
        )
        return DimensionTableInfo(
            **table_dict,
            dimension_id_column=dimension_table.dimension_id_column,
        )
