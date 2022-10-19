"""
DataStatusService
"""
from __future__ import annotations

from typing import Union

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import DataStatus
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.event_data import EventDataUpdate
from featurebyte.schema.item_data import ItemDataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.base_update import BaseUpdateService


class DataStatusService(BaseUpdateService):
    """
    DataStatusService is responsible to update the data status.
    """

    @staticmethod
    async def _update_data_status(
        service: Union[BaseDocumentService[EventDataModel], BaseDocumentService[ItemDataModel]],
        document_id: ObjectId,
        data: Union[EventDataUpdate, ItemDataUpdate],
    ) -> None:
        document = await service.get_document(document_id=document_id)

        # check eligibility of status transition
        eligible_transitions = {
            DataStatus.DRAFT: {DataStatus.PUBLISHED},
            DataStatus.PUBLISHED: {DataStatus.DEPRECATED},
            DataStatus.DEPRECATED: {},
        }
        current_status = document.status
        if data.status is not None and current_status != data.status:
            if data.status not in eligible_transitions[current_status]:
                raise DocumentUpdateError(
                    f"Invalid status transition from {current_status} to {data.status}."
                )
            await service.update_document(document_id=document_id, data=data, return_document=False)

    async def update_event_data_status(self, document_id: ObjectId, status: DataStatus) -> None:
        """
        Update event data status

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        status: DataStatus
            Data status
        """
        await self._update_data_status(
            self.event_data_service, document_id=document_id, data=EventDataUpdate(status=status)
        )

    async def update_item_data_status(self, document_id: ObjectId, status: DataStatus) -> None:
        """
        Update item data status

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        status: DataStatus
            Data status
        """
        await self._update_data_status(
            self.item_data_service, document_id=document_id, data=ItemDataUpdate(status=status)
        )
