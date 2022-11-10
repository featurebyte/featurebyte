"""
DataStatusService
"""
from __future__ import annotations

from typing import List, Union, cast

from collections import defaultdict

from bson.objectid import ObjectId

from featurebyte.enum import TableDataType
from featurebyte.exception import DocumentUpdateError
from featurebyte.models.feature_store import DataModel, DataStatus
from featurebyte.schema.dimension_data import DimensionDataUpdate
from featurebyte.schema.entity import EntityServiceUpdate
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

            async with self.persistent.start_transaction():

                await service.update_document(
                    document_id=document_id,
                    data=type(data)(columns_info=data.columns_info),  # type: ignore
                    return_document=False,
                )

                await self.update_entity_data_references(document, data)

    async def update_entity_data_references(
        self, document: DataModel, data: DataUpdateSchema
    ) -> None:
        """
        Update data columns info

        Parameters
        ----------
        document: DataModel
            DataModel object
        data: DataUpdateSchema
            Data upload payload
        """
        if not data.columns_info:
            return

        # update data references in affected entities
        primary_keys_cols_mapping = {
            TableDataType.EVENT_DATA: ["event_id_column"],
            TableDataType.ITEM_DATA: [],
            TableDataType.DIMENSION_DATA: ["dimension_data_id_column"],
        }
        primary_keys_cols = cast(List[str], primary_keys_cols_mapping.get(document.type, []))
        primary_keys = [getattr(document, key_col) for key_col in primary_keys_cols]

        entity_update: dict[ObjectId, int] = defaultdict(int)
        primary_entity_update: dict[ObjectId, int] = defaultdict(int)
        for column_info in document.columns_info:
            if column_info.entity_id:
                # flag for removal from entity
                entity_update[column_info.entity_id] -= 1
                if column_info.name in primary_keys:
                    primary_entity_update[column_info.entity_id] -= 1
        for column_info in data.columns_info:
            if column_info.entity_id:
                # flag for addition to entity
                entity_update[column_info.entity_id] += 1
                if column_info.name in primary_keys:
                    primary_entity_update[column_info.entity_id] += 1

        for entity_id, update_flag in entity_update.items():
            primary_update_flag = primary_entity_update.get(entity_id, 0)
            if update_flag == 0 and primary_update_flag == 0:
                continue

            # data reference update is needed
            entity = await self.entity_service.get_document(entity_id)
            update_values = {}
            if update_flag != 0:
                # data references updated in entity
                update_action = (
                    self.include_object_id if update_flag > 0 else self.exclude_object_id
                )
                update_values["tabular_data_ids"] = update_action(
                    document_ids=entity.tabular_data_ids, document_id=document.id
                )
            if primary_update_flag != 0:
                # primary data references updated in entity
                update_action = (
                    self.include_object_id if primary_update_flag > 0 else self.exclude_object_id
                )
                update_values["primary_tabular_data_ids"] = update_action(
                    document_ids=entity.primary_tabular_data_ids, document_id=document.id
                )
            await self.entity_service.update_document(
                document_id=entity_id,
                data=EntityServiceUpdate(**update_values),
            )
