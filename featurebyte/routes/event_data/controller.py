"""
EventData API route controller
"""
from __future__ import annotations

from typing import Any, Type

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.exception import DocumentConflictError, DocumentNotFoundError, DocumentUpdateError
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

        Raises
        ------
        HTTPException
            If some referenced object not found or there exists conflicting value
        """
        try:
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).create_document(data)
            return document
        except DocumentNotFoundError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
        except DocumentConflictError as exc:
            raise HTTPException(status_code=HTTPStatus.CONFLICT, detail=str(exc)) from exc

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

        Raises
        ------
        HTTPException
            Invalid event data status transition
        """
        try:
            document = await cls.document_service_class(
                user=user, persistent=persistent
            ).update_document(document_id=event_data_id, data=data)
            return document
        except DocumentNotFoundError as exc:
            raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail=str(exc)) from exc
        except DocumentUpdateError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY, detail=str(exc)
            ) from exc
