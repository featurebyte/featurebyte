"""
ItemDataService class
"""
from __future__ import annotations

from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate
from featurebyte.service.base_data import BaseDataDocumentService


class ItemDataService(BaseDataDocumentService[ItemDataModel, ItemDataCreate, ItemDataUpdate]):
    """
    ItemDataService class
    """

    document_class = ItemDataModel
