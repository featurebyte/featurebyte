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
from featurebyte.service.semantic import SemanticService
from featurebyte.service.table_columns_info import TableColumnsInfoService, TableDocumentService
from featurebyte.service.table_info import TableInfoService
from featurebyte.service.table_status import TableStatusService


class SCDTableController(BaseTableDocumentController[SCDTableModel, SCDTableService, SCDTableList]):
    """
    SCDTable controller
    """

    paginated_document_class = SCDTableList
    document_update_schema_class = SCDTableServiceUpdate

    def __init__(
        self,
        service: TableDocumentService,
        table_columns_info_service: TableColumnsInfoService,
        table_status_service: TableStatusService,
        semantic_service: SemanticService,
        table_info_service: TableInfoService,
    ):
        super().__init__(
            service, table_columns_info_service, table_status_service, semantic_service
        )
        self.table_info_service = table_info_service

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
        scd_table = await self.service.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=scd_table, verbose=verbose
        )
        return SCDTableInfo(
            **table_dict,
            natural_key_column=scd_table.natural_key_column,
            effective_timestamp_column=scd_table.effective_timestamp_column,
            surrogate_key_column=scd_table.surrogate_key_column,
            end_timestamp_column=scd_table.end_timestamp_column,
            current_flag_column=scd_table.current_flag_column,
        )
