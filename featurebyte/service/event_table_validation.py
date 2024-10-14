"""
EventTableValidationService class
"""

from __future__ import annotations

from featurebyte.models.event_table import EventTableModel
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate
from featurebyte.service.base_table_validation import BaseTableValidationService


class EventTableValidationService(
    BaseTableValidationService[EventTableModel, EventTableCreate, EventTableServiceUpdate]
):
    """
    EventTableValidationService class
    """
