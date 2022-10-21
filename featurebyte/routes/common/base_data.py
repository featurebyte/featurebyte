"""
BaseDataController for API routes
"""
from __future__ import annotations

from typing import Type, TypeVar, Union, overload

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.schema.data import DataUpdate
from featurebyte.schema.event_data import EventDataCreate, EventDataUpdate
from featurebyte.schema.item_data import ItemDataCreate, ItemDataUpdate
from featurebyte.service.data_update import DataUpdateService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.info import InfoService
from featurebyte.service.item_data import ItemDataService

DataDocumentT = TypeVar("DataDocumentT", EventDataModel, ItemDataModel)
DataDocumentServiceT = TypeVar("DataDocumentServiceT", EventDataService, ItemDataService)


class BaseDataDocumentController(
    BaseDocumentController[DataDocumentT, DataDocumentServiceT, PaginatedDocument]
):
    """
    BaseDataDocumentController for API routes
    """

    document_update_schema_class: Type[DataUpdate]

    def __init__(
        self,
        service: Union[EventDataService, ItemDataService],
        data_update_service: DataUpdateService,
        info_service: InfoService,
    ):
        super().__init__(service)  # type: ignore[arg-type]
        self.data_update_service = data_update_service
        self.info_service = info_service

    @overload
    async def create_data(self, data: EventDataCreate) -> EventDataModel:
        ...

    @overload
    async def create_data(self, data: ItemDataCreate) -> ItemDataModel:
        ...

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
        return await self.service.create_document(data)  # type: ignore[arg-type]

    @overload
    async def update_data(self, document_id: ObjectId, data: EventDataUpdate) -> EventDataModel:
        ...

    @overload
    async def update_data(self, document_id: ObjectId, data: ItemDataUpdate) -> ItemDataModel:
        ...

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
                service=self.service,
                document_id=document_id,
                data=data,
            )

        if data.status:
            await self.data_update_service.update_data_status(
                service=self.service,
                document_id=document_id,
                data=data,
            )

        update_dict = data.dict(exclude={"status": True, "columns_info": True}, exclude_none=True)
        if update_dict:
            await self.service.update_document(
                document_id=document_id,
                data=self.document_update_schema_class(**update_dict),  # type: ignore[arg-type]
                return_document=False,
            )

        return await self.get(document_id=document_id)
