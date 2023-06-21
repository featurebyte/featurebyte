"""
DimensionTableService class
"""
from __future__ import annotations

from typing import Any

from bson import ObjectId

from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.persistent import Persistent
from featurebyte.schema.dimension_table import DimensionTableCreate, DimensionTableServiceUpdate
from featurebyte.schema.info import DimensionTableInfo
from featurebyte.service.base_table_document import BaseTableDocumentService
from featurebyte.service.table_info import TableInfoService


class DimensionTableService(
    BaseTableDocumentService[DimensionTableModel, DimensionTableCreate, DimensionTableServiceUpdate]
):
    """
    DimensionTableService class
    """

    document_class = DimensionTableModel
    document_update_class = DimensionTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "DimensionTable"

    def __init__(
        self,
        user: Any,
        persistent: Persistent,
        catalog_id: ObjectId,
        table_info_service: TableInfoService,
    ):
        super().__init__(user, persistent, catalog_id)
        self.table_info_service = table_info_service

    async def get_dimension_table_info(
        self, document_id: ObjectId, verbose: bool
    ) -> DimensionTableInfo:
        """
        Get dimension table info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Verbose or not

        Returns
        -------
        DimensionTableInfo
        """
        dimension_table = await self.get_document(document_id=document_id)
        table_dict = await self.table_info_service.get_table_info(
            data_document=dimension_table, verbose=verbose
        )
        return DimensionTableInfo(
            **table_dict,
            dimension_id_column=dimension_table.dimension_id_column,
        )
