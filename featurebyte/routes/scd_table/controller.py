"""
SCDTable API route controller
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.info import SCDTableInfo
from featurebyte.schema.scd_table import SCDTableList, SCDTableServiceUpdate
from featurebyte.service.entity import EntityService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.scd_table import SCDTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.target import TargetService


class SCDTableController(BaseTableDocumentController[SCDTableModel, SCDTableService, SCDTableList]):
    """
    SCDTable controller
    """

    paginated_document_class = SCDTableList
    document_update_schema_class = SCDTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "effective_timestamp_column": SemanticType.SCD_EFFECTIVE_TIMESTAMP,
        "current_flag_column": SemanticType.SCD_CURRENT_FLAG,
        "surrogate_key_column": SemanticType.SCD_SURROGATE_KEY_ID,
        "natural_key_column": SemanticType.SCD_NATURAL_KEY_ID,
        "end_timestamp_column": SemanticType.SCD_END_TIMESTAMP,
    }

    def __init__(
        self,
        scd_table_service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        table_info_service: TableInfoService,
        entity_service: EntityService,
        feature_service: FeatureService,
        target_service: TargetService,
        feature_list_service: FeatureListService,
        specialized_dtype_detection_service: SpecializedDtypeDetectionService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=scd_table_service,
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

    async def get_info(self, document_id: ObjectId, verbose: bool) -> SCDTableInfo:
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
        SCDTableInfo
        """
        scd_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=scd_table, verbose=verbose
        )
        return SCDTableInfo(
            **table_dict,
            natural_key_column=scd_table.natural_key_column,
            effective_timestamp_column=scd_table.effective_timestamp_column,
            surrogate_key_column=scd_table.surrogate_key_column,
            end_timestamp_column=scd_table.end_timestamp_column,
            current_flag_column=scd_table.current_flag_column,
        )
