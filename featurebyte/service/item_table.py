"""
ItemTableService class
"""

from __future__ import annotations

from bson import ObjectId

from featurebyte.models.item_table import ItemTableModel
from featurebyte.schema.item_table import ItemTableCreate, ItemTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.event_table import EventTableService


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


class ExtendedItemTableService:
    """
    ExtendedItemTableService class
    """

    def __init__(
        self, item_table_service: ItemTableService, event_table_service: EventTableService
    ):
        self.item_table_service = item_table_service
        self.event_table_service = event_table_service

    async def get_document_with_event_table_model(self, document_id: ObjectId) -> ItemTableModel:
        """
        Get ItemTableModel with the event_table_model field populated

        Parameters
        ----------
        document_id: ObjectId
            Document ID

        Returns
        -------
        ItemTableModel
        """
        item_table = await self.item_table_service.get_document(document_id=document_id)
        item_table.event_table_model = await self.event_table_service.get_document(
            document_id=item_table.event_table_id
        )
        return item_table
