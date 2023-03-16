"""
ItemTable API route controller
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.item_table import ItemTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.info import ItemTableInfo
from featurebyte.schema.item_table import ItemTableList, ItemTableServiceUpdate
from featurebyte.service.item_table import ItemTableService


class ItemTableController(
    BaseTableDocumentController[ItemTableModel, ItemTableService, ItemTableList]
):
    """
    ItemTable controller
    """

    paginated_document_class = ItemTableList
    document_update_schema_class = ItemTableServiceUpdate

    async def _get_column_semantic_map(self, document: ItemTableModel) -> dict[str, Any]:
        item_id = await self.semantic_service.get_or_create_document(name=SemanticType.ITEM_ID)
        return {document.item_id_column: item_id}

    async def get_info(self, document_id: ObjectId, verbose: bool) -> ItemTableInfo:
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
        ItemTableInfo
        """
        info_document = await self.info_service.get_item_table_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
