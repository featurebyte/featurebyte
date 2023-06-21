"""
ItemTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.item_table import ItemTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.info import ItemTableInfo
from featurebyte.schema.item_table import ItemTableCreate, ItemTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.event_table import EventTableService
from featurebyte.service.table_info import TableInfoService


class ItemTableService(
    BaseTableDocumentService[ItemTableModel, ItemTableCreate, ItemTableServiceUpdate]
):
    """
    ItemTableService class
    """

    document_class = ItemTableModel
    document_update_class = ItemTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "ItemTable"

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        table_info_service: TableInfoService,
        event_table_service: EventTableService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.table_info_service = table_info_service
        self.event_table_service = event_table_service

    async def get_item_table_info(self, document_id: ObjectId, verbose: bool) -> ItemTableInfo:
        """
        Get item table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        ItemTableInfo
        """
        item_table = await self.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=item_table, verbose=verbose
        )
        event_table = await self.event_table_service.get_document(
            document_id=item_table.event_table_id
        )
        return ItemTableInfo(
            **table_dict,
            event_id_column=item_table.event_id_column,
            item_id_column=item_table.item_id_column,
            event_table_name=event_table.name,
        )
