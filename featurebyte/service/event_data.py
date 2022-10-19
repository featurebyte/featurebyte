"""
EventDataService class
"""
from __future__ import annotations

from bson.objectid import ObjectId

from featurebyte.models.event_data import EventDataModel
from featurebyte.schema.common.operation import DictProject
from featurebyte.schema.entity import EntityBriefInfoList
from featurebyte.schema.event_data import (
    EventDataColumnInfo,
    EventDataCreate,
    EventDataInfo,
    EventDataUpdate,
)
from featurebyte.service.base_data import BaseDataDocumentService
from featurebyte.service.base_document import GetInfoServiceMixin
from featurebyte.service.entity import EntityService


class EventDataService(
    BaseDataDocumentService[EventDataModel, EventDataCreate, EventDataUpdate],
    GetInfoServiceMixin[EventDataInfo],
):
    """
    EventDataService class
    """

    document_class = EventDataModel

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
