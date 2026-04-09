"""
ForecastTableService class
"""

from __future__ import annotations

from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.schema.forecast_table import ForecastTableCreate, ForecastTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class ForecastTableService(
    BaseTableDocumentService[ForecastTableModel, ForecastTableCreate, ForecastTableServiceUpdate]
):
    """
    ForecastTableService class
    """

    document_class = ForecastTableModel
    document_update_class = ForecastTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "ForecastTable"
