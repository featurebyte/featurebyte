"""
ItemDataService class
"""
from __future__ import annotations

from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class ItemDataService(BaseDataDocumentService[ItemDataModel, ItemDataCreate, ItemDataUpdate]):
    """
    ItemDataService class
    """

    document_class = ItemDataModel


register_service_constructor(ItemDataService)
