"""
EventData API routes
"""
# pylint: disable=too-few-public-methods,relative-beyond-top-level
from typing import List, Literal, Optional

import datetime
from http import HTTPStatus

from beanie import PydanticObjectId
from bson.objectid import ObjectId
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from featurebyte.models.event_data import (
    DatabaseSource,
    EventDataModel,
    EventDataStatus,
    FeatureJobSetting,
    FeatureJobSettingHistoryEntry,
)
from featurebyte.persistent import DuplicateDocumentError

from .schema import PaginationMixin

router = APIRouter()
TABLE_NAME = "event_data"


class EventDataCreate(BaseModel):
    """
    Event Data Creation Payload

    Parameters
    ----------
    """

    name: str
    table_name: str
    source: DatabaseSource
    event_timestamp_column: str
    record_creation_date_column: Optional[str]
    default_feature_job_setting: Optional[FeatureJobSetting]


class EventData(EventDataModel):
    """
    Event Data

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
        Configuration for Event Data schema
        """

        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class EventDatas(PaginationMixin):
    """
    Paginated list of Event Datas
    """

    data: List[EventData]

    class Config:
        """
        Configuration for Event Datas schema
        """

        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        json_encoders = {ObjectId: str}


class EventDataUpdate(BaseModel):
    """
    Event Data update schema
    """

    default_feature_job_setting: Optional[FeatureJobSetting]
    status: Optional[EventDataStatus]


@router.post("/event_data", response_model=EventData, status_code=HTTPStatus.CREATED)
def create_event_data(
    request: Request,
    data: EventDataCreate,
) -> EventData:
    """
    Create Event Data
    """
    user = request.state.user
    persistent = request.state.persistent

    # ensure table name does not already exist
    query_filter = {"name": data.name, "user_id": user.id}
    if persistent.find_one(collection_name=TABLE_NAME, filter_query=query_filter):
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f'Event Data "{data.name}" already exists.',
        )

    # init history and set status to draft
    document = EventData(
        user_id=user.id,
        created_at=datetime.datetime.utcnow(),
        status=EventDataStatus.DRAFT,
        history=[
            FeatureJobSettingHistoryEntry(
                creation_date=datetime.datetime.utcnow(), setting=data.default_feature_job_setting
            )
        ],
        **data.dict(),
    )
    try:
        persistent.insert_one(collection_name=TABLE_NAME, document=document.dict())
    except DuplicateDocumentError as exc:
        raise HTTPException(
            status_code=HTTPStatus.UNPROCESSABLE_ENTITY,
            detail=f'Event Data "{data.name}" already exists.',
        ) from exc

    return EventData(**persistent.find_one(collection_name=TABLE_NAME, filter_query=query_filter))


@router.get("/event_data", response_model=EventDatas)
def list_event_datas(
    request: Request,
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    search: Optional[str] = None,
) -> EventDatas:
    """
    List Event Datas
    """
    user = request.state.user
    persistent = request.state.persistent

    query_filter = {"user_id": user.id}

    # Apply search
    if search:
        query_filter["$text"] = {"$search": search}

    docs, total = persistent.find(
        collection_name=TABLE_NAME,
        filter_query=query_filter,
        sort_by=sort_by,
        sort_dir=sort_dir,
        page=page,
        page_size=page_size,
    )
    return EventDatas(page=page, page_size=page_size, total=total, data=list(docs))


@router.get("/event_data/{event_data_name}", response_model=EventData)
def retrieve_event_data(
    request: Request,
    event_data_name: str,
) -> Optional[EventData]:
    """
    Retrieve Event Data
    """
    user = request.state.user
    persistent = request.state.persistent

    query_filter = {"name": event_data_name, "user_id": user.id}
    event_data = persistent.find_one(collection_name=TABLE_NAME, filter_query=query_filter)
    if not event_data:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail=f'Event Data "{event_data_name}" not found.'
        )
    return EventData(**event_data)


@router.patch("/event_data/{event_data_name}", response_model=EventData)
def update_event_data(
    request: Request,
    event_data_name: str,
    data: EventDataUpdate,
) -> EventData:
    """
    Update scheduled task
    """
    user = request.state.user
    persistent = request.state.persistent

    query_filter = {"name": event_data_name, "user_id": user.id}
    event_data = persistent.find_one(collection_name=TABLE_NAME, filter_query=query_filter)
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
        collection_name=TABLE_NAME, filter_query=query_filter, update={"$set": update_payload}
    )
    if not updated_cnt:
        raise not_found_exception

    return EventData(**persistent.find_one(collection_name=TABLE_NAME, filter_query=query_filter))
