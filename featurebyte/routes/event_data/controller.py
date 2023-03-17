"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any

from bson.objectid import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.event_data import EventDataModel
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.event_data import EventDataList, EventDataServiceUpdate
from featurebyte.schema.info import EventDataInfo
from featurebyte.service.event_data import EventDataService


class EventDataController(
    BaseDataDocumentController[EventDataModel, EventDataService, EventDataList]
):
    """
    EventData controller
    """

    paginated_document_class = EventDataList
    document_update_schema_class = EventDataServiceUpdate

    async def _get_column_semantic_map(self, document: EventDataModel) -> dict[str, Any]:
        event_timestamp = await self.semantic_service.get_or_create_document(
            name=SemanticType.EVENT_TIMESTAMP
        )
        event_id = await self.semantic_service.get_or_create_document(name=SemanticType.EVENT_ID)
        assert document.event_id_column is not None
        return {
            document.event_timestamp_column: event_timestamp,
            document.event_id_column: event_id,
        }

    async def get_info(self, document_id: ObjectId, verbose: bool) -> EventDataInfo:
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
        EventDataInfo
        """
        info_document = await self.info_service.get_event_data_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
