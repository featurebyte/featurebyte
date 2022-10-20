"""
EventDataService class
"""
from __future__ import annotations

from featurebyte.models.event_data import EventDataModel
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate
from featurebyte.service.base_data import BaseDataDocumentService


class EventDataService(BaseDataDocumentService[EventDataModel, EventDataCreate, EventDataUpdate]):
    """
    EventDataService class
    """

    document_class = EventDataModel
