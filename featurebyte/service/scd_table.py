"""
SCDTableService class
"""

from __future__ import annotations

from featurebyte.models.scd_table import SCDTableModel
from featurebyte.schema.scd_table import SCDTableCreate, SCDTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class SCDTableService(
    BaseTableDocumentService[SCDTableModel, SCDTableCreate, SCDTableServiceUpdate]
):
    """
    SCDTableService class
    """

    document_class = SCDTableModel
    document_update_class = SCDTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "SCDTable"
