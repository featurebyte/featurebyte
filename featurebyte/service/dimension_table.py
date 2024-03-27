"""
DimensionTableService class
"""

from __future__ import annotations

from featurebyte.models.dimension_table import DimensionTableModel
from featurebyte.schema.dimension_table import DimensionTableCreate, DimensionTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


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
