"""
EventData API routes
"""
from __future__ import annotations

from typing import Any, Literal

from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.enum import CollectionName
from featurebyte.models.event_data import EventDataStatus, FeatureJobSettingHistoryEntry
from featurebyte.persistent import DuplicateDocumentError, Persistent
from featurebyte.routes.common.helpers import get_utc_now
from featurebyte.schema.event_data import EventData, EventDataCreate, EventDataList, EventDataUpdate


class EventDataController:
    """
    EventData controller
    """

    collection_name = CollectionName.EVENT_DATA

    @classmethod
    def create_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        data: EventDataCreate,
    ) -> EventData:
        """
        Create Event Data
        """
        # exclude microseconds from timestamp as it's not supported in persistent
        utc_now = get_utc_now()

        # init history and set status to draft
        if data.default_feature_job_setting:
            history = [
                FeatureJobSettingHistoryEntry(
                    creation_date=utc_now,
                    setting=data.default_feature_job_setting,
                )
            ]
        else:
            history = []

        document = EventData(
            user_id=user.id,
            created_at=utc_now,
            status=EventDataStatus.DRAFT,
            history=history,
            **data.dict(),
        )
        try:
            insert_id = persistent.insert_one(
                collection_name=cls.collection_name, document=document.dict(by_alias=True)
            )
            assert insert_id == document.id
        except DuplicateDocumentError as exc:
            raise HTTPException(
                status_code=HTTPStatus.CONFLICT,
                detail=f'Event Data "{data.name}" already exists.',
            ) from exc

        return document

    @classmethod
    def list_event_datas(
        cls,
        user: Any,
        persistent: Persistent,
        page: int = 1,
        page_size: int = 10,
        sort_by: str | None = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: str | None = None,
        name: str | None = None,
    ) -> EventDataList:
        """
        List Event Datas
        """
        query_filter = {"user_id": user.id}

        if name is not None:
            query_filter["name"] = name

        # Apply search
        if search:
            query_filter["$text"] = {"$search": search}

        try:
            docs, total = persistent.find(
                collection_name=cls.collection_name,
                query_filter=query_filter,
                sort_by=sort_by,
                sort_dir=sort_dir,
                page=page,
                page_size=page_size,
            )
            return EventDataList(page=page, page_size=page_size, total=total, data=list(docs))
        except NotImplementedError as exc:
            raise HTTPException(
                status_code=HTTPStatus.NOT_IMPLEMENTED, detail="Query not supported."
            ) from exc

    @classmethod
    def retrieve_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        event_data_id: str,
    ) -> EventData:
        """
        Retrieve Event Data
        """
        query_filter = {"_id": ObjectId(event_data_id), "user_id": user.id}
        event_data = persistent.find_one(
            collection_name=cls.collection_name, query_filter=query_filter
        )
        if event_data is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'Event Data ID "{event_data_id}" not found.',
            )
        return EventData(**event_data)

    @classmethod
    def update_event_data(
        cls,
        user: Any,
        persistent: Persistent,
        event_data_id: str,
        data: EventDataUpdate,
    ) -> EventData:
        """
        Update scheduled task
        """
        query_filter = {"_id": ObjectId(event_data_id), "user_id": user.id}
        event_data = persistent.find_one(
            collection_name=cls.collection_name, query_filter=query_filter
        )
        not_found_exception = HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Event Data ID "{event_data_id}" not found.'
        )
        if not event_data:
            raise not_found_exception

        # prepare update payload
        update_payload = data.dict()
        if data.default_feature_job_setting:
            update_payload["history"] = [
                FeatureJobSettingHistoryEntry(
                    creation_date=get_utc_now(),
                    setting=update_payload["default_feature_job_setting"],
                ).dict()
            ] + event_data["history"]
        else:
            update_payload.pop("default_feature_job_setting")

        if data.status:
            # check eligibility of status transition
            eligible_transitions = {
                EventDataStatus.DRAFT: {EventDataStatus.PUBLISHED},
                EventDataStatus.PUBLISHED: {EventDataStatus.DEPRECATED},
                EventDataStatus.DEPRECATED: {},
            }
            current_status = event_data["status"]
            if data.status not in eligible_transitions[current_status]:
                raise HTTPException(
                    status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                    detail=f"Invalid status transition from {current_status} to {data.status}.",
                )
        else:
            update_payload.pop("status")

        updated_cnt = persistent.update_one(
            collection_name=cls.collection_name,
            query_filter=query_filter,
            update={"$set": update_payload},
        )
        if not updated_cnt:
            raise not_found_exception

        event_data = persistent.find_one(
            collection_name=cls.collection_name, query_filter=query_filter
        )
        if event_data is None:
            raise not_found_exception
        return EventData(**event_data)
