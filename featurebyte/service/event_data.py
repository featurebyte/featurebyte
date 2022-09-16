"""
EventDataService class
"""
from __future__ import annotations

from typing import Any, Optional

from bson.objectid import ObjectId

from featurebyte.exception import DocumentUpdateError
from featurebyte.models.event_data import EventDataModel, EventDataStatus
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.entity import EntityBriefInfoList
from featurebyte.schema.event_data import (
    EventDataColumnInfo,
    EventDataCreate,
    EventDataInfo,
    EventDataUpdate,
)
from featurebyte.service.base_document import BaseDocumentService, GetInfoServiceMixin
from featurebyte.service.entity import EntityService
from featurebyte.service.feature_store import FeatureStoreService


class EventDataService(BaseDocumentService[EventDataModel], GetInfoServiceMixin[EventDataInfo]):
    """
    EventDataService class
    """

    document_class = EventDataModel

    async def create_document(  # type: ignore[override]
        self, data: EventDataCreate, get_credential: Any = None
    ) -> EventDataModel:
        _ = get_credential
        _ = await FeatureStoreService(user=self.user, persistent=self.persistent).get_document(
            document_id=data.tabular_source.feature_store_id
        )
        document = EventDataModel(
            user_id=self.user.id, status=EventDataStatus.DRAFT, **data.json_dict()
        )

        # check any conflict with existing documents
        await self._check_document_unique_constraints(document=document)
        insert_id = await self.persistent.insert_one(
            collection_name=self.collection_name,
            document=document.dict(by_alias=True),
            user_id=self.user.id,
        )
        assert insert_id == document.id
        return await self.get_document(document_id=insert_id)

    async def update_document(  # type: ignore[override]
        self,
        document_id: ObjectId,
        data: EventDataUpdate,
        document: Optional[EventDataModel] = None,
        return_document: bool = True,
    ) -> Optional[EventDataModel]:
        if document is None:
            document = await self.get_document(document_id=document_id)

        # prepare update payload
        update_payload = data.dict()

        # check eligibility of status transition
        eligible_transitions = {
            EventDataStatus.DRAFT: {EventDataStatus.PUBLISHED},
            EventDataStatus.PUBLISHED: {EventDataStatus.DEPRECATED},
            EventDataStatus.DEPRECATED: {},
        }
        current_status = document.status
        if (
            current_status != data.status
            and data.status not in eligible_transitions[current_status]
        ):
            raise DocumentUpdateError(
                f"Invalid status transition from {current_status} to {data.status}."
            )

        await self.persistent.update_one(
            collection_name=self.collection_name,
            query_filter=self._construct_get_query_filter(document_id=document_id),
            update={"$set": update_payload},
            user_id=self.user.id,
        )

        if return_document:
            return await self.get_document(document_id=document_id)
        return None

    async def get_info(self, document_id: ObjectId, verbose: bool) -> EventDataInfo:
        event_data = await self.get_document(document_id=document_id)
        entity_service = EntityService(user=self.user, persistent=self.persistent)
        entity_ids = DictProject(rule=("columns_info", "entity_id")).project(event_data.dict())
        entities = await entity_service.list_documents(
            page=1, page_size=0, query_filter={"_id": {"$in": entity_ids}}
        )
        columns_info = None
        if verbose:
            columns_info = []
            entity_map = {entity["_id"]: entity["name"] for entity in entities["data"]}
            for column_info in event_data.columns_info:
                columns_info.append(
                    EventDataColumnInfo(
                        **column_info.dict(), entity=entity_map.get(column_info.entity_id)
                    )
                )

        return EventDataInfo(
            name=event_data.name,
            created_at=event_data.created_at,
            updated_at=event_data.updated_at,
            event_timestamp_column=event_data.event_timestamp_column,
            record_creation_date_column=event_data.record_creation_date_column,
            table_details=event_data.tabular_source.table_details,
            default_feature_job_setting=event_data.default_feature_job_setting,
            status=event_data.status,
            entities=EntityBriefInfoList.from_paginated_data(entities),
            column_count=len(event_data.columns_info),
            columns_info=columns_info,
        )
