"""
DimensionTable API route controller
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.dimension_table import DimensionTableList, DimensionTableServiceUpdate
from featurebyte.schema.info import DimensionTableInfo
from featurebyte.service.dimension_table import DimensionTableService
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableColumnsInfoService, TableDocumentService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.table_status import TableStatusService


class DimensionTableController(
    BaseTableDocumentController[DimensionTableModel, DimensionTableService, DimensionTableList]
):
    """
    DimensionTable controller
    """

    paginated_document_class = DimensionTableList
    document_update_schema_class = DimensionTableServiceUpdate

    def __init__(
        self,
        dimension_table_service: TableDocumentService,
        table_columns_info_service: TableColumnsInfoService,
        table_status_service: TableStatusService,
        semantic_service: SemanticService,
        table_info_service: TableInfoService,
    ):
        super().__init__(
            dimension_table_service,
            table_columns_info_service,
            table_status_service,
            semantic_service,
        )
        self.table_info_service = table_info_service

    async def _get_column_semantic_map(self, document: DimensionTableModel) -> dict[str, Any]:
        dimension_id_semantic = await self.semantic_service.get_or_create_document(
            name=SemanticType.DIMENSION_ID
        )
        return {document.dimension_id_column: dimension_id_semantic}

    async def get_info(self, document_id: ObjectId, verbose: bool) -> DimensionTableInfo:
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
        DimensionTableInfo
        """
        dimension_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=dimension_table, verbose=verbose
        )
        return DimensionTableInfo(
            **table_dict,
            dimension_id_column=dimension_table.dimension_id_column,
        )
