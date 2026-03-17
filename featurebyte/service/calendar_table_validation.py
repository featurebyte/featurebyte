"""
CalendarTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.calendar_table import CalendarTableModel
from featurebyte.schema.calendar_table import CalendarTableCreate, CalendarTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


class CalendarTableValidationService(
    BaseTableValidationService[CalendarTableModel, CalendarTableCreate, CalendarTableServiceUpdate]
):
    """
    CalendarTableValidationService class
    """
