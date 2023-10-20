"""
EventTable API route controller
"""
from __future__ import annotations

from typing import Any, List, Tuple

from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.event_table import EventTableModel
from featurebyte.models.persistent import QueryFilter
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.event_table import EventTableList, EventTableServiceUpdate
from featurebyte.schema.info import EventTableInfo
from featurebyte.service.entity import EntityService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.feature import FeatureService
from featurebyte.service.feature_job_setting_analysis import FeatureJobSettingAnalysisService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService


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
        table_info_service: TableInfoService,
        entity_service: EntityService,
        item_table_service: ItemTableService,
        feature_service: FeatureService,
        feature_job_setting_analysis_service: FeatureJobSettingAnalysisService,
    ):
        super().__init__(event_table_service, table_facade_service, semantic_service)
        self.table_info_service = table_info_service
        self.entity_service = entity_service
        self.item_table_service = item_table_service
        self.feature_service = feature_service
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
        return [
            (self.entity_service, {"primary_table_ids": document_id}),
            (self.item_table_service, {"event_table_id": document_id}),
            (self.feature_service, {"table_ids": document_id}),
            (self.feature_job_setting_analysis_service, {"event_table_id": document_id}),
        ]
