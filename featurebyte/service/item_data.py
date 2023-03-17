"""
ItemDataService class
"""
from __future__ import annotations

from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.item_data import ItemDataCreate, ItemDataServiceUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class ItemDataService(
    BaseDataDocumentService[ItemDataModel, ItemDataCreate, ItemDataServiceUpdate]
):
    """
    ItemDataService class
    """

    document_class = ItemDataModel
