"""
EventData API route controller
"""
from __future__ import annotations

from featurebyte.models.event_data import EventDataModel
from featurebyte.routes.common.base import GetInfoControllerMixin
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.event_data import EventDataInfo, EventDataList, EventDataUpdate


class EventDataController(  # type: ignore[misc]
    BaseDataDocumentController[EventDataModel, EventDataList], GetInfoControllerMixin[EventDataInfo]
):
    """
    EventData controller
    """

    paginated_document_class = EventDataList
    document_update_schema_class = EventDataUpdate
