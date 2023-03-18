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


class DimensionTableController(
    BaseTableDocumentController[DimensionTableModel, DimensionTableService, DimensionTableList]
):
    """
    DimensionTable controller
    """

    paginated_document_class = DimensionTableList
    document_update_schema_class = DimensionTableServiceUpdate

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
        info_document = await self.info_service.get_dimension_table_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
