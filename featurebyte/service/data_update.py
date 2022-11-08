"""
DataStatusService
"""
from __future__ import annotations

from typing import Union

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_store import DataStatus
from featurebyte.schema.dimension_data import DimensionDataUpdate
from featurebyte.schema.event_data import EventDataUpdate
from featurebyte.schema.item_data import ItemDataUpdate
from featurebyte.service.base_service import BaseService
from featurebyte.service.dimension_data import DimensionDataService
from featurebyte.service.entity import EntityService
from featurebyte.service.event_data import EventDataService
from featurebyte.service.item_data import ItemDataService
from featurebyte.service.semantic import SemanticService

DataDocumentService = Union[EventDataService, ItemDataService, DimensionDataService]
DataUpdateSchema = Union[EventDataUpdate, ItemDataUpdate, DimensionDataUpdate]


class DataUpdateService(BaseService):
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
                data=type(data)(status=data.status),  # type: ignore
                return_document=False,
            )

    @staticmethod
    async def _validate_column_info_id_field_values(
        data: DataUpdateSchema,
        field_name: str,
        service: Union[EntityService, SemanticService],
        field_class_name: str,
    ) -> None:
        assert data.columns_info is not None
        id_values = [
            getattr(col_info, field_name)
            for col_info in data.columns_info
            if getattr(col_info, field_name)
        ]
        docs = await service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": id_values}}
        )
        found_id_values = [ObjectId(doc["_id"]) for doc in docs["data"]]
        missing_id_values = sorted(set(id_values).difference(found_id_values))
        if missing_id_values:
            column_name_id_pairs = sorted(
                [
                    (col_info.name, getattr(col_info, field_name))
                    for col_info in data.columns_info
                    if getattr(col_info, field_name) in missing_id_values
                ]
            )
            col_names, id_vals = zip(*column_name_id_pairs)
            id_vals = [str(id_val) for id_val in id_vals]
            raise DocumentUpdateError(
                f"{field_class_name} IDs {list(id_vals)} not found for columns {list(col_names)}."
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
        """
        document = await service.get_document(document_id=document_id)

        if data.columns_info is not None and data.columns_info != document.columns_info:
            # check existence of the entities & semantics
            await self._validate_column_info_id_field_values(
                data=data,
                field_name="entity_id",
                service=self.entity_service,
                field_class_name="Entity",
            )
            await self._validate_column_info_id_field_values(
                data=data,
                field_name="semantic_id",
                service=self.semantic_service,
                field_class_name="Semantic",
            )

            await service.update_document(
                document_id=document_id,
                data=type(data)(columns_info=data.columns_info),  # type: ignore
                return_document=False,
            )
