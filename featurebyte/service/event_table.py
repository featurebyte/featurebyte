"""
EventTableService class
"""

from __future__ import annotations

from featurebyte.models.event_table import EventTableModel
from featurebyte.schema.event_table import EventTableCreate, EventTableServiceUpdate
from featurebyte.service.base_table_document import BaseTableDocumentService


class EventTableService(
    BaseTableDocumentService[EventTableModel, EventTableCreate, EventTableServiceUpdate]
):
    """
    EventTableService class
    """

    document_class = EventTableModel
    document_update_class = EventTableServiceUpdate

    @property
    def class_name(self) -> str:
        return "EventTable"
