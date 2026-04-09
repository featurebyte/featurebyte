"""
ForecastTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.forecast_table import ForecastTableModel
from featurebyte.schema.forecast_table import ForecastTableCreate, ForecastTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


class ForecastTableValidationService(
    BaseTableValidationService[ForecastTableModel, ForecastTableCreate, ForecastTableServiceUpdate]
):
    """
    ForecastTableValidationService class
    """
