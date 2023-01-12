"""
ItemData API route controller
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.info import ItemDataInfo
from featurebyte.schema.item_data import ItemDataList, ItemDataServiceUpdate
from featurebyte.service.item_data import ItemDataService


class ItemDataController(BaseDataDocumentController[ItemDataModel, ItemDataService, ItemDataList]):
    """
    ItemData controller
    """

    paginated_document_class = ItemDataList
    document_update_schema_class = ItemDataServiceUpdate

    async def _get_column_semantic_map(self, document: ItemDataModel) -> dict[str, Any]:
        item_id = await self.semantic_service.get_or_create_document(name=SemanticType.ITEM_ID)
        return {document.item_id_column: item_id}

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ItemDataInfo:
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
        ItemDataInfo
        """
        info_document = await self.info_service.get_item_data_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
