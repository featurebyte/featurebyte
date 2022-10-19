"""
ItemData API route controller
"""
from __future__ import annotations

from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.item_data import ItemDataList, ItemDataUpdate


class ItemDataController(BaseDataDocumentController[ItemDataModel, ItemDataList]):
    """
    ItemData controller
    """

    paginated_document_class = ItemDataList
    document_update_schema_class = ItemDataUpdate
