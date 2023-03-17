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


class EventTableController(
    BaseTableDocumentController[EventTableModel, EventTableService, EventTableList]
):
    """
    EventTable controller
    """

    paginated_document_class = EventTableList
    document_update_schema_class = EventTableServiceUpdate

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
        info_document = await self.info_service.get_event_table_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
