"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any, Type

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.persistent import Persistent
from featurebyte.routes.common.base import BaseDocumentController
from featurebyte.schema.event_data import EventDataCreate, EventDataList, EventDataUpdate
from featurebyte.service.event_data import EventDataService


class EventDataController(BaseDocumentController[EventDataModel, EventDataList]):
    """
    EventData controller
    """

    paginated_document_class = EventDataList
    document_service_class: Type[EventDataService] = EventDataService  # type: ignore[assignment]

    @classmethod
    async def create_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        data: EventDataCreate,
    ) -> EventDataModel:
        """
        Create Event Data at persistent

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        data: EventDataCreate
            EventData creation payload

        Returns
        -------
        EventDataModel
            Newly created event data object
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).create_document(data)
        return document

    @classmethod
    async def update_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        event_data_id: ObjectId,
        data: EventDataUpdate,
    ) -> EventDataModel:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        user: Any
            User class to provide user identifier
        persistent: Persistent
            Object that entity will be saved to
        event_data_id: ObjectId
            EventData ID
        data: EventDataUpdate
            Event data update payload

        Returns
        -------
        EventDataModel
            EventData object with updated attribute(s)
        """
        document = await cls.document_service_class(
            user=user, persistent=persistent
        ).update_document(document_id=event_data_id, data=data)
        return document
