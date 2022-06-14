"""
EventTable API routes
"""
# pylint: disable=too-few-public-methods,relative-beyond-top-level
from typing import Any, List, Literal, Optional

import datetime
from http import HTTPStatus

from bson.objectid import ObjectId
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from featurebyte.models.event_table import (
    EventTableModel,
    FeatureJobSetting,
    FeatureJobSettingHistoryEntry,
)

from .schema import PaginationMixin, PydanticObjectId
from .unified_api_settings import session_user, storage

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

    default_feature_job_setting: FeatureJobSetting


@router.post("/event_table", response_model=EventTable, status_code=HTTPStatus.CREATED)
def create_event_table(
    data: EventTableModel,
    user: Any = Depends(session_user),  # pylint: disable=unused-argument
) -> EventTable:
    """
    Create event table
    """
    # ensure table name does not already exist
    query_filter = {"name": data.name, "user_id": user.id}
    if storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter):
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f'Event table "{data.name}" already exists.',
        )

    document = EventTable(user_id=user.id, **data.dict())
    storage.insert_one(collection_name=TABLE_NAME, document=document.dict())
    return EventTable(**storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter))


@router.get("/event_table", response_model=EventTables)
def list_event_tables(
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    search: Optional[str] = None,
    user: Any = Depends(session_user),  # pylint: disable=unused-argument
) -> EventTables:
    """
    List event tables
    """
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
    event_table_name: str,
    user: Any = Depends(session_user),  # pylint: disable=unused-argument
) -> Optional[EventTable]:
    """
    Retrieve event table
    """
    query_filter = {"name": event_table_name, "user_id": user.id}
    event_table = storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter)
    if not event_table:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Event table "{event_table_name}" not found.'
        )
    return EventTable(**event_table)


@router.patch("/event_table/{event_table_name}", response_model=EventTable)
def update_event_table(
    event_table_name: str,
    data: EventTableUpdate,
    user: Any = Depends(session_user),  # pylint: disable=unused-argument
) -> EventTable:
    """
    Update scheduled task
    """
    query_filter = {"name": event_table_name, "user_id": user.id}
    event_table = storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter)
    not_found_exception = HTTPException(
        status_code=HTTPStatus.NOT_FOUND, detail=f'Event table "{event_table_name}" not found.'
    )
    if not event_table:
        raise not_found_exception

    # prepare update payload
    update_payload = data.dict()
    update_payload["history"] = [
        FeatureJobSettingHistoryEntry(
            creation_date=datetime.datetime.utcnow(),
            setting=update_payload["default_feature_job_setting"],
        ).dict()
    ] + event_table["history"]

    updated_cnt = storage.update_one(
        collection_name=TABLE_NAME, filter_query=query_filter, update={"$set": update_payload}
    )
    if not updated_cnt:
        raise not_found_exception

    return EventTable(**storage.find_one(collection_name=TABLE_NAME, filter_query=query_filter))
