"""
EventData API route controller
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.routes.common.base_data import BaseDataDocumentController
from featurebyte.schema.event_data import EventDataInfo, EventDataList, EventDataUpdate


class EventDataController(BaseDataDocumentController[EventDataModel, EventDataList]):
    """
    EventData controller
    """

    paginated_document_class = EventDataList
    document_update_schema_class = EventDataUpdate

    async def get_info(
        self,
        document_id: ObjectId,
        verbose: bool,
    ) -> EventDataInfo:
        """
        Get document info given document ID

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        verbose: bool
            Flag to control verbose level

        Returns
        -------
        InfoDocument
        """
        info_document = await self.info_service.get_event_data_info(
            document_id=document_id, verbose=verbose
        )
        return info_document
