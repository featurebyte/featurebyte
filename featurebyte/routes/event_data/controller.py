"""
EventData API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

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

    async def update_data(  # type: ignore
        self, document_id: ObjectId, data: EventDataUpdate
    ) -> EventDataModel:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            EventData ID
        data: EventDataUpdate
            Event data update payload

        Returns
        -------
        EventDataModel
            EventData object with updated attribute(s)
        """
        await super().update_data(document_id=document_id, data=data)

        update_dict = data.dict(exclude={"status": True, "columns_info": True}, exclude_none=True)
        if update_dict:
            await self.service.update_document(
                document_id=document_id,
                data=EventDataUpdate(**update_dict),
                return_document=False,
            )

        return await self.get(document_id=document_id)
