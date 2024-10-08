"""
EventTable API route controller
"""

from __future__ import annotations

from typing import Any, List, Tuple

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.routes.task.controller import TaskController
from featurebyte.schema.event_table import EventTableList, EventTableServiceUpdate
from featurebyte.schema.info import EventTableInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.feature_list import FeatureListService
from featurebyte.service.feature_store import FeatureStoreService
from featurebyte.service.feature_store_warehouse import FeatureStoreWarehouseService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.specialized_dtype import SpecializedDtypeDetectionService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.target import TargetService


class EventTableController(
    BaseTableDocumentController[EventTableModel, EventTableService, EventTableList]
):
    """
    EventTable controller
    """

    paginated_document_class = EventTableList
    document_update_schema_class = EventTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "event_id_column": SemanticType.EVENT_ID,
        "event_timestamp_column": SemanticType.EVENT_TIMESTAMP,
        "event_timestamp_timezone_offset_column": SemanticType.TIME_ZONE,
    }

    def __init__(
        self,
        event_table_service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        entity_service: EntityService,
        feature_service: FeatureService,
        target_service: TargetService,
        feature_list_service: FeatureListService,
        table_info_service: TableInfoService,
        item_table_service: ItemTableService,
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
        specialized_dtype_detection_service: SpecializedDtypeDetectionService,
        feature_store_service: FeatureStoreService,
        feature_store_warehouse_service: FeatureStoreWarehouseService,
        task_controller: TaskController,
    ):
        super().__init__(
            service=event_table_service,
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
        self.item_table_service = item_table_service
        self.feature_job_setting_analysis_service = feature_job_setting_analysis_service

    async def get_info(self, document_id: ObjectId, verbose: bool) -> EventTableInfo:
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
        EventTableInfo
        """
        event_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=event_table, verbose=verbose
        )
        return EventTableInfo(
            **table_dict,
            event_id_column=event_table.event_id_column,
            event_timestamp_column=event_table.event_timestamp_column,
            default_feature_job_setting=event_table.default_feature_job_setting,
        )

    async def service_and_query_pairs_for_checking_reference(
        self, document_id: ObjectId
    ) -> List[Tuple[Any, QueryFilter]]:
        return await super().service_and_query_pairs_for_checking_reference(
            document_id=document_id
        ) + [
            (self.item_table_service, {"event_table_id": document_id}),
            (self.feature_job_setting_analysis_service, {"event_table_id": document_id}),
        ]
