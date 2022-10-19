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
from featurebyte.service.columns_info import ColumnsInfoService
from featurebyte.service.data_status import DataStatusService
from featurebyte.service.event_data import EventDataService


class EventDataController(  # type: ignore[misc]
    BaseDocumentController[EventDataModel, EventDataList], GetInfoControllerMixin[EventDataInfo]
):
    """
    EventData controller
    """

    paginated_document_class = EventDataList

    def __init__(
        self,
        service: EventDataService,
        columns_info_service: ColumnsInfoService,
        data_status_service: DataStatusService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.columns_info_service = columns_info_service
        self.data_status_service = data_status_service

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
        if data.columns_info:
            await self.columns_info_service.update_event_data_columns_info(
                document_id=event_data_id, columns_info=data.columns_info
            )

        if data.status:
            await self.data_status_service.update_event_data_status(
                document_id=event_data_id, status=data.status
            )

        if data.dict(exclude={"status": True, "columns_info": True}, exclude_none=True):
            await self.service.update_document(
                document_id=event_data_id,
                data=EventDataUpdate(**data.dict(exclude={"status": True})),
                return_document=False,
            )

        return await self.get(document_id=event_data_id)
