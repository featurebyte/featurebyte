"""
EventTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.event_table import EventTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate
from featurebyte.schema.info import EventTableInfo
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.table_info import TableInfoService


class EventTableService(
    BaseTableDocumentService[EventTableModel, EventTableCreate, EventTableServiceUpdate]
):
    """
    EventTableService class
    """

    document_class = EventTableModel
    document_update_class = EventTableServiceUpdate

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        table_info_service: TableInfoService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.table_info_service = table_info_service

    @property
    def class_name(self) -> str:
        return "EventTable"

    async def get_event_table_info(self, document_id: ObjectId, verbose: bool) -> EventTableInfo:
        """
        Get event table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        EventTableInfo
        """
        event_table = await self.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=event_table, verbose=verbose
        )
        return EventTableInfo(
            **table_dict,
            event_id_column=event_table.event_id_column,
            event_timestamp_column=event_table.event_timestamp_column,
            default_feature_job_setting=event_table.default_feature_job_setting,
        )
