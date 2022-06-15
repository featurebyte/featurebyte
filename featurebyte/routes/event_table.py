"""
EventTable API routes
"""
# pylint: disable=too-few-public-methods,relative-beyond-top-level
from typing import List, Literal, Optional

import datetime
from http import HTTPStatus

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from featurebyte.models.event_table import (
    EventTableModel,
    EventTableStatus,
    FeatureJobSetting,
    FeatureJobSettingHistoryEntry,
)

from .schema import PaginationMixin

router = APIRouter()
TABLE_NAME = "event_table"


class EventTable(EventTableModel):
    """
    Event Table

    Parameters
    ----------
    id: ObjectId
        Document identifier
    user_id: ObjectId
        User identifier
    """

    id: PydanticObjectId = Field(default_factory=ObjectId, alias="_id")
    user_id: PydanticObjectId

    class Config:
        """
        Configuration for Event Table schema
        """

        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class EventTables(PaginationMixin):
    """
    Paginated list of Event Tables
    """

    data: List[EventTable]

    class Config:
        """
        Configuration for Event Tables schema
        """

        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class EventTableUpdate(BaseModel):
    """
    Event table update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
    status: Optional[EventTableStatus]


@router.post("/event_table", response_model=EventTable, status_code=HTTPStatus.CREATED)
def create_event_table(
    request: Request,
    data: EventTableModel,
) -> EventTable:
    """
    Create event table
    """
    user = request.state.user
    storage = request.state.storage

    # ensure table name does not already exist
    query_filter = {"name": data.name, "user_id": user.id}
    if storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter):
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f'Event table "{data.name}" already exists.',
        )

    # init history and set status to draft
    data.history = [
        FeatureJobSettingHistoryEntry(
            creation_date=datetime.datetime.utcnow(), setting=data.default_feature_job_setting
        )
    ]
    data.status = EventTableStatus.DRAFT

    document = EventTable(user_id=user.id, **data.dict())
    storage.insert_one(collection_name=TABLE_NAME, document=document.dict())
    return EventTable(**storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter))


@router.get("/event_table", response_model=EventTables)
def list_event_tables(
    request: Request,
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    search: Optional[str] = None,
) -> EventTables:
    """
    List event tables
    """
    user = request.state.user
    storage = request.state.storage

    query_filter = {"user_id": user.id}

    # Apply search
    if search:
        query_filter["$text"] = {"$search": search}

    docs, total = storage.find(
        collection_name=TABLE_NAME,
        filter_query=query_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )
    return EventTables(page=page, page_size=page_size, total=total, data=list(docs))


@router.get("/event_table/{event_table_name}", response_model=EventTable)
def retrieve_event_table(
    request: Request,
    event_table_name: str,
) -> Optional[EventTable]:
    """
    Retrieve event table
    """
    user = request.state.user
    storage = request.state.storage

    query_filter = {"name": event_table_name, "user_id": user.id}
    event_table = storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter)
    if not event_table:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Event table "{event_table_name}" not found.'
        )
    return EventTable(**event_table)


@router.patch("/event_table/{event_table_name}", response_model=EventTable)
def update_event_table(
    request: Request,
    event_table_name: str,
    data: EventTableUpdate,
) -> EventTable:
    """
    Update scheduled task
    """
    user = request.state.user
    storage = request.state.storage

    query_filter = {"name": event_table_name, "user_id": user.id}
    event_table = storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter)
    not_found_exception = HTTPException(
        status_code=HTTPStatus.NOT_FOUND, detail=f'Event table "{event_table_name}" not found.'
    )
    if not event_table:
        raise not_found_exception

    # prepare update payload
    update_payload = data.dict()
    if data.default_feature_job_setting:
        update_payload["history"] = [
            FeatureJobSettingHistoryEntry(
                creation_date=datetime.datetime.utcnow(),
                setting=update_payload["default_feature_job_setting"],
            ).dict()
        ] + event_table["history"]
    else:
        update_payload.pop("default_feature_job_setting")

    if data.status:
        # check eligibility of status transition
        eligible_transitions = {
            EventTableStatus.DRAFT: {EventTableStatus.PUBLISHED},
            EventTableStatus.PUBLISHED: {EventTableStatus.DEPRECATED},
            EventTableStatus.DEPRECATED: {},
        }
        current_status = event_table["status"]
        if data.status not in eligible_transitions[current_status]:
            raise HTTPException(
                status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
                detail=f"Invalid status transition from {current_status} to {data.status}.",
            )
    else:
        update_payload.pop("status")

    updated_cnt = storage.update_one(
        collection_name=TABLE_NAME, filter_query=query_filter, update={"$set": update_payload}
    )
    if not updated_cnt:
        raise not_found_exception

    return EventTable(**storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter))
