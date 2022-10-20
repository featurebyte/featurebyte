"""
BaseDataController for API routes
"""
from __future__ import annotations

from typing import Type, Union

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.models.item_data import ItemDataModel
from featurebyte.routes.common.base import BaseDocumentController, PaginatedDocument
from featurebyte.schema.data import DataUpdate
from featurebyte.schema.event_data import EventDataCreate
from featurebyte.schema.item_data import ItemDataCreate
from featurebyte.service.base_document import Document
from featurebyte.service.data_update import DataUpdateSchema, DataUpdateService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.info import InfoService
from featurebyte.service.item_data import ItemDataService

DataCreateSchema = Union[EventDataCreate, ItemDataCreate]
DataModelSchema = Union[EventDataModel, ItemDataModel]


class BaseDataDocumentController(BaseDocumentController[Document, PaginatedDocument]):
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

    async def create_data(self, data: DataCreateSchema) -> DataModelSchema:
        """
        Create Data at persistent

        Parameters
        ----------
        data: DataCreateSchema
            EventData or ItemData creation payload

        Returns
        -------
        DataModelSchema
            Newly created data object
        """
        document = await self.service.create_document(data)
        return document  # type: ignore

    async def update_data(self, document_id: ObjectId, data: DataUpdateSchema) -> DataModelSchema:
        """
        Update EventData (for example, to update scheduled task) at persistent (GitDB or MongoDB)

        Parameters
        ----------
        document_id: ObjectId
            Data document ID
        data: DataUpdateSchema
            Data update payload

        Returns
        -------
        DataModelSchema
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

        update_dict = data.dict(exclude={"status": True, "columns_info": True}, exclude_none=True)
        if update_dict:
            await self.service.update_document(
                document_id=document_id,
                data=self.document_update_schema_class(**update_dict),
                return_document=False,
            )

        return await self.get(document_id=document_id)  # type: ignore
