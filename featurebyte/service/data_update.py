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

DataDocumentService = Union[BaseDocumentService[EventDataModel], BaseDocumentService[ItemDataModel]]
DataUpdateSchema = Union[EventDataUpdate, ItemDataUpdate]


class DataUpdateService(BaseUpdateService):
    """
    DataStatusService is responsible to update the data status.
    """

    @staticmethod
    async def update_data_status(
        service: DataDocumentService, document_id: ObjectId, data: DataUpdateSchema
    ) -> None:
        """
        Update data status

        Parameters
        ----------
        service: DataDocumentService
            Data service object
        document_id: ObjectId
            Document ID
        data: DataUpdateSchema
            Data upload payload

        Raises
        ------
        DocumentUpdateError
            When the data status transition is invalid
        """
        document = await service.get_document(document_id=document_id)

        current_status = document.status
        if data.status is not None and current_status != data.status:
            # check eligibility of status transition
            eligible_transitions = {
                DataStatus.DRAFT: {DataStatus.PUBLISHED},
                DataStatus.PUBLISHED: {DataStatus.DEPRECATED},
                DataStatus.DEPRECATED: {},
            }
            if data.status not in eligible_transitions[current_status]:
                raise DocumentUpdateError(
                    f"Invalid status transition from {current_status} to {data.status}."
                )
            await service.update_document(
                document_id=document_id,
                data=type(data)(status=data.status),
                return_document=False,
            )

    async def update_columns_info(
        self, service: DataDocumentService, document_id: ObjectId, data: DataUpdateSchema
    ) -> None:
        """
        Update data columns info

        Parameters
        ----------
        service: DataDocumentService
            Data service object
        document_id: ObjectId
            Document ID
        data: DataUpdateSchema
            Data upload payload

        Raises
        ------
        DocumentUpdateError
            When there exists some entity IDs cannot be found
        """
        document = await service.get_document(document_id=document_id)

        if data.columns_info is not None and data.columns_info != document.columns_info:
            # check existence of the entity ids
            entity_ids = [column.entity_id for column in data.columns_info if column.entity_id]
            entities = await self.entity_service.list_documents(
                page=1, page_size=0, query_filter={"_id": {"$in": entity_ids}}
            )
            found_entities = [ObjectId(doc["_id"]) for doc in entities["data"]]
            missing_entities = sorted(set(entity_ids).difference(found_entities))
            if missing_entities:
                column_names = sorted(
                    [
                        column.name
                        for column in data.columns_info
                        if column.entity_id in missing_entities
                    ]
                )
                raise DocumentUpdateError(
                    f"Entity IDs {missing_entities} not found for columns {column_names}."
                )
            await service.update_document(
                document_id=document_id,
                data=type(data)(columns_info=data.columns_info),
                return_document=False,
            )
