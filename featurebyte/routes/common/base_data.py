"""
BaseDataController for API routes
"""
from __future__ import annotations

from typing import Union

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate
from featurebyte.service.base_document import Document
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.item_data import ItemDataService


class BaseDataDocumentController(BaseDocumentController[Document, PaginatedDocument]):
    """
    BaseDataDocumentController for API routes
    """

    def __init__(
        self,
        service: Union[EventDataService, ItemDataService],
        data_update_service: DataUpdateService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.data_update_service = data_update_service

    async def create_data(
        self, data: Union[EventDataCreate, ItemDataCreate]
    ) -> Union[EventDataModel, ItemDataModel]:
        """
        Create Data at persistent

        Parameters
        ----------
        data: Union[EventDataCreate, ItemDataCreate]
            EventData or ItemData creation payload

        Returns
        -------
        Union[EventDataModel, ItemDataModel]
            Newly created data object
        """
        document = await self.service.create_document(data)
        return document  # type: ignore

    async def update_data(
        self, document_id: ObjectId, data: Union[EventDataUpdate, ItemDataUpdate]
    ) -> Union[EventDataModel, ItemDataModel]:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Data document ID
        data: Union[EventDataUpdate, ItemDataUpdate]
            Data update payload

        Returns
        -------
        Union[EventDataModel, ItemDataModel]
            Data object with updated attribute(s)
        """
        if data.columns_info:
            await self.data_update_service.update_columns_info(
                service=self.service,  # type: ignore
                document_id=document_id,
                data=data,
            )

        if data.status:
            await self.data_update_service.update_data_status(
                service=self.service,  # type: ignore
                document_id=document_id,
                data=data,
            )

        return await self.get(document_id=document_id)  # type: ignore
