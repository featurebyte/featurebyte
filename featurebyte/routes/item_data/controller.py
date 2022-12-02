"""
ItemData API route controller
"""
from __future__ import annotations

from typing import Any

from featurebyte.enum import SemanticType
from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.app_container import register_controller_constructor
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.item_data import ItemDataList, ItemDataUpdate
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.info import InfoService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.semantic import SemanticService


class ItemDataController(BaseDataDocumentController[ItemDataModel, ItemDataService, ItemDataList]):
    """
    ItemData controller
    """

    paginated_document_class = ItemDataList
    document_update_schema_class = ItemDataUpdate

    async def _get_column_semantic_map(self, document: ItemDataModel) -> dict[str, Any]:
        item_id = await self.semantic_service.get_or_create_document(name=SemanticType.ITEM_ID)
        return {document.item_id_column: item_id}


register_controller_constructor(
    ItemDataController, [ItemDataService, DataUpdateService, SemanticService, InfoService]
)
