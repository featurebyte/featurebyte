"""
SCDTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.scd_table import SCDTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.info import SCDTableInfo
from featurebyte.schema.scd_table import SCDTableCreate, SCDTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.table_info import TableInfoService


class SCDTableService(
    BaseTableDocumentService[SCDTableModel, SCDTableCreate, SCDTableServiceUpdate]
):
    """
    SCDTableService class
    """

    document_class = SCDTableModel
    document_update_class = SCDTableServiceUpdate

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        table_info_service: TableInfoService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.table_info_service = table_info_service

    @property
    def class_name(self) -> str:
        return "SCDTable"

    async def get_scd_table_info(self, document_id: ObjectId, verbose: bool) -> SCDTableInfo:
        """
        Get Slow Changing Dimension table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        SCDTableInfo
        """
        scd_table = await self.get_document(document_id=document_id)
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
