"""
ItemTable API route controller
"""
from __future__ import annotations

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.item_table import ItemTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.info import ItemTableInfo
from featurebyte.schema.item_table import ItemTableList, ItemTableServiceUpdate
from featurebyte.service.event_table import EventTableService
from featurebyte.service.item_table import ItemTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableDocumentService
from featurebyte.service.table_facade import TableFacadeService
from featurebyte.service.table_info import TableInfoService


class ItemTableController(
    BaseTableDocumentController[ItemTableModel, ItemTableService, ItemTableList]
):
    """
    ItemTable controller
    """

    paginated_document_class = ItemTableList
    document_update_schema_class = ItemTableServiceUpdate
    semantic_tag_rules = {
        **BaseTableDocumentController.semantic_tag_rules,
        "event_id_column": SemanticType.EVENT_ID,
        "item_id_column": SemanticType.ITEM_ID,
    }

    def __init__(
        self,
        item_table_service: TableDocumentService,
        table_facade_service: TableFacadeService,
        semantic_service: SemanticService,
        table_info_service: TableInfoService,
        event_table_service: EventTableService,
    ):
        super().__init__(item_table_service, table_facade_service, semantic_service)
        self.table_info_service = table_info_service
        self.event_table_service = event_table_service

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
        item_table = await self.service.get_document(document_id=document_id)
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
