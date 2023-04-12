"""
ItemTableService class
"""
from __future__ import annotations

from featurebyte.models.item_table import ItemTableModel
from featurebyte.schema.item_table import ItemTableCreate, ItemTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


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
