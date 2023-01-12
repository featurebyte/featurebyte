"""
EventDataService class
"""
from __future__ import annotations

from featurebyte.models.event_data import EventDataModel
from featurebyte.schema.event_data import EventDataCreate, EventDataServiceUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class EventDataService(
    BaseDataDocumentService[EventDataModel, EventDataCreate, EventDataServiceUpdate]
):
    """
    EventDataService class
    """

    document_class = EventDataModel
