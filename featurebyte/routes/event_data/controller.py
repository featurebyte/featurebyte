"""
EventData API routes
"""
from typing import Any, Literal, Optional

import datetime
from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import HTTPException

from featurebyte.models.event_data import EventDataStatus, FeatureJobSettingHistoryEntry
from featurebyte.persistent import DuplicateDocumentError, Persistent

from .schema import (
    EventDataCreate,
    EventDataDocument,
    EventDataList,
    EventDataRead,
    EventDataUpdate,
)

TABLE_NAME = "event_data"


class EventDataController:
    """
    EventData controller
    """

    @staticmethod
    def create_event_data(
        user: Any,
        persistent: Persistent,
        data: EventDataCreate,
    ) -> EventDataDocument:
        """
        Create Event Data
        """
        # exclude microseconds from timestamp as it's not supported in persistent
        utc_now = datetime.datetime.utcnow()
        utc_now = utc_now.replace(microsecond=int(utc_now.microsecond / 1000) * 1000)

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

        document = EventDataDocument(
            user_id=user.id,
            created_at=utc_now,
            status=EventDataStatus.DRAFT,
            history=history,
            **data.dict(),
        )
        try:
            insert_id = persistent.insert_one(
                collection_name=TABLE_NAME, document=document.dict(by_alias=True)
            )
            assert isinstance(insert_id, ObjectId)
            setattr(document, "id", insert_id)
        except DuplicateDocumentError as exc:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail=f'Event Data "{data.name}" already exists.',
            ) from exc

        return EventDataRead(**document.dict())

    @staticmethod
    def list_event_datas(
        user: Any,
        persistent: Persistent,
        page: int = 1,
        page_size: int = 10,
        sort_by: Optional[str] = "created_at",
        sort_dir: Literal["asc", "desc"] = "desc",
        search: Optional[str] = None,
    ) -> EventDataList:
        """
        List Event Datas
        """
        query_filter = {"user_id": user.id}

        # Apply search
        if search:
            query_filter["$text"] = {"$search": search}

        docs, total = persistent.find(
            collection_name=TABLE_NAME,
            query_filter=query_filter,
            sort_by=sort_by,
            sort_dir=sort_dir,
            page=page,
            page_size=page_size,
        )
        return EventDataList(page=page, page_size=page_size, total=total, data=list(docs))

    @staticmethod
    def retrieve_event_data(
        user: Any,
        persistent: Persistent,
        event_data_name: str,
    ) -> Optional[EventDataRead]:
        """
        Retrieve Event Data
        """
        query_filter = {"name": event_data_name, "user_id": user.id}
        event_data = persistent.find_one(collection_name=TABLE_NAME, query_filter=query_filter)
        if event_data is None:
            raise HTTPException(
                status_code=HTTPStatus.NOT_FOUND,
                detail=f'Event Data "{event_data_name}" not found.',
            )
        return EventDataRead(**event_data)

    @staticmethod
    def update_event_data(
        user: Any,
        persistent: Persistent,
        event_data_name: str,
        data: EventDataUpdate,
    ) -> EventDataRead:
        """
        Update scheduled task
        """
        query_filter = {"name": event_data_name, "user_id": user.id}
        event_data = persistent.find_one(collection_name=TABLE_NAME, query_filter=query_filter)
        not_found_exception = HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Event Data "{event_data_name}" not found.'
        )
        if not event_data:
            raise not_found_exception

        # prepare update payload
        update_payload = data.dict()
        if data.default_feature_job_setting:
            update_payload["history"] = [
                FeatureJobSettingHistoryEntry(
                    creation_date=datetime.datetime.utcnow(),
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
            collection_name=TABLE_NAME, query_filter=query_filter, update={"$set": update_payload}
        )
        if not updated_cnt:
            raise not_found_exception

        event_data = persistent.find_one(collection_name=TABLE_NAME, query_filter=query_filter)
        if not event_data:
            raise not_found_exception
        return EventDataRead(**event_data)
