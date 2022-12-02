"""
EventDataService class
"""
from __future__ import annotations

from featurebyte.models.event_data import EventDataModel
from featurebyte.routes.app_container import register_service_constructor
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate
from featurebyte.service.base_data_document import BaseDataDocumentService


class EventDataService(BaseDataDocumentService[EventDataModel, EventDataCreate, EventDataUpdate]):
    """
    EventDataService class
    """

    document_class = EventDataModel


register_service_constructor(EventDataService)
