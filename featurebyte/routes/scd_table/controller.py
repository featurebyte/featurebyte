"""
SCDTable API route controller
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.enum import SemanticType
from featurebyte.models.scd_table import SCDTableModel
from featurebyte.routes.common.base_table import BaseTableDocumentController
from featurebyte.schema.info import SCDTableInfo
from featurebyte.schema.scd_table import SCDTableList, SCDTableServiceUpdate
from featurebyte.service.scd_table import SCDTableService


class SCDTableController(BaseTableDocumentController[SCDTableModel, SCDTableService, SCDTableList]):
    """
    SCDTable controller
    """

    paginated_document_class = SCDTableList
    document_update_schema_class = SCDTableServiceUpdate

    async def _get_column_semantic_map(self, document: SCDTableModel) -> dict[str, Any]:
        scd_natural_key_id = await self.semantic_service.get_or_create_document(
            name=SemanticType.SCD_NATURAL_KEY_ID
        )
        scd_surrogate_key_id = await self.semantic_service.get_or_create_document(
            name=SemanticType.SCD_SURROGATE_KEY_ID
        )
        column_semantic_map = {
            document.natural_key_column: scd_natural_key_id,
        }
        if document.surrogate_key_column:
            column_semantic_map.update(
                {
                    document.surrogate_key_column: scd_surrogate_key_id,
                }
            )
        return column_semantic_map

    async def get_info(self, document_id: ObjectId, verbose: bool) -> SCDTableInfo:
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
        SCDTableInfo
        """
        info_document = await self.info_service.get_scd_table_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
