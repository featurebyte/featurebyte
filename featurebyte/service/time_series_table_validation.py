"""
TimeSeriesTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.time_series_table import TimeSeriesTableModel
from featurebyte.schema.time_series_table import TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


class TimeSeriesTableValidationService(
    BaseTableValidationService[
        TimeSeriesTableModel, TimeSeriesTableCreate, TimeSeriesTableServiceUpdate
    ]
):
    """
    TimeSeriesTableValidationService class
    """
