"""
TimeSeriesTableService class
"""

from __future__ import annotations

from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.schema.time_series_table import TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class TimeSeriesTableService(
    BaseTableDocumentService[
        TimeSeriesTableModel, TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
    ]
):
    """
    TimeSeriesTableService class
    """

    document_class = TimeSeriesTableModel
    document_update_class = TimeSeriesTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "TimeSeriesTable"
