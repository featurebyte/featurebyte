"""
ColumnInfoService
"""
from __future__ import annotations

from typing import List, Union

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.event_data import EventDataModel
from featurebyte.models.feature_store import ColumnInfo
from featurebyte.models.item_data import ItemDataModel
from featurebyte.schema.event_data import EventDataUpdate
from featurebyte.schema.item_data import ItemDataUpdate
from featurebyte.service.base_document import BaseDocumentService
from featurebyte.service.base_update import BaseUpdateService


class ColumnsInfoService(BaseUpdateService):
    """
    ColumnsInfoService is responsible to update the columns info of the data
    """

    async def _update_columns_info(
        self,
        service: Union[BaseDocumentService[EventDataModel], BaseDocumentService[ItemDataModel]],
        document_id: ObjectId,
        data: Union[EventDataUpdate, ItemDataUpdate],
    ) -> None:
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
            await service.update_document(document_id=document_id, data=data, return_document=False)

    async def update_event_data_columns_info(
        self, document_id: ObjectId, columns_info: List[ColumnInfo]
    ) -> None:
        """
        Update event data columns info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        columns_info: List[ColumnInfo]
            List of columns info
        """
        await self._update_columns_info(
            self.event_data_service,
            document_id=document_id,
            data=EventDataUpdate(columns_info=columns_info),
        )

    async def update_item_data_columns_info(
        self, document_id: ObjectId, columns_info: List[ColumnInfo]
    ) -> None:
        """
        Update item data columns info

        Parameters
        ----------
        document_id: ObjectId
            Document ID
        columns_info: List[ColumnInfo]
            List of columns info
        """
        await self._update_columns_info(
            self.item_data_service,
            document_id=document_id,
            data=ItemDataUpdate(columns_info=columns_info),
        )
