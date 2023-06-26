"""
EventTable API route controller
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.event_table import EventTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.event_table import EventTableList, EventTableServiceUpdate
from featurebyte.schema.info import EventTableInfo
from featurebyte.service.event_table import EventTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableColumnsInfoService, TableDocumentService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.table_status import TableStatusService


class EventTableController(
    BaseTableDocumentController[EventTableModel, EventTableService, EventTableList]
):
    """
    EventTable controller
    """

    paginated_document_class = EventTableList
    document_update_schema_class = EventTableServiceUpdate

    def __init__(
        self,
        event_table_service: TableDocumentService,
        table_columns_info_service: TableColumnsInfoService,
        table_status_service: TableStatusService,
        semantic_service: SemanticService,
        table_info_service: TableInfoService,
    ):
        super().__init__(
            event_table_service, table_columns_info_service, table_status_service, semantic_service
        )
        self.table_info_service = table_info_service

    async def _get_column_semantic_map(self, document: EventTableModel) -> dict[str, Any]:
        event_timestamp = await self.semantic_service.get_or_create_document(
            name=SemanticType.EVENT_TIMESTAMP
        )
        event_id = await self.semantic_service.get_or_create_document(name=SemanticType.EVENT_ID)
        assert document.event_id_column is not None
        return {
            document.event_timestamp_column: event_timestamp,
            document.event_id_column: event_id,
        }

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
