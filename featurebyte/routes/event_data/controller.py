"""
EventData API route controller
"""
from __future__ import annotations

from typing import cast

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.routes.common.base import BaseDocumentController, GetInfoControllerMixin
from featurebyte.schema.event_data import (
    EventDataCreate,
    EventDataInfo,
    EventDataList,
    EventDataUpdate,
)
from featurebyte.service.event_data import EventDataService


class EventDataController(  # type: ignore[misc]
    BaseDocumentController[EventDataModel, EventDataList], GetInfoControllerMixin[EventDataInfo]
):
    """
    EventData controller
    """

    paginated_document_class = EventDataList

    def __init__(self, service: EventDataService):
        super().__init__(service)  # type: ignore[arg-type]

    async def create_event_data(
        self,
        data: EventDataCreate,
    ) -> EventDataModel:
        """
        Create Event Data at persistent

        Parameters
        ----------
        data: EventDataCreate
            EventData creation payload

        Returns
        -------
        EventDataModel
            Newly created event data object
        """
        document = await self.service.create_document(data)
        return cast(EventDataModel, document)

    async def update_event_data(
        self,
        event_data_id: ObjectId,
        data: EventDataUpdate,
    ) -> EventDataModel:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        event_data_id: ObjectId
            EventData ID
        data: EventDataUpdate
            Event data update payload

        Returns
        -------
        EventDataModel
            EventData object with updated attribute(s)
        """
        document = await self.service.update_document(document_id=event_data_id, data=data)
        assert document is not None
        return cast(EventDataModel, document)
