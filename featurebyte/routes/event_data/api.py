"""
EventData API routes
"""
from __future__ import annotations

from typing import Literal, Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.event_data import EventDataModel
from featurebyte.schema.event_data import EventDataCreate, EventDataList, EventDataUpdate

router = APIRouter(prefix="/event_data")


@router.post("", response_model=EventDataModel, status_code=HTTPStatus.CREATED)
async def create_event_data(
    request: Request,
    data: EventDataCreate,
) -> EventDataModel:
    """
    Create Event Data
    """
    event_data: EventDataModel = await request.state.controller.create_event_data(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return event_data


@router.get("", response_model=EventDataList)
async def list_event_datas(
    request: Request,
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    search: Optional[str] = None,
    name: Optional[str] = None,
) -> EventDataList:
    """
    List Event Datas
    """
    event_data_list: EventDataList = await request.state.controller.list_event_datas(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return event_data_list


@router.get("/{event_data_id}", response_model=EventDataModel)
async def retrieve_event_data(
    request: Request,
    event_data_id: str,
) -> EventDataModel:
    """
    Retrieve Event Data
    """
    event_data: EventDataModel = await request.state.controller.retrieve_event_data(
        user=request.state.user,
        persistent=request.state.persistent,
        event_data_id=event_data_id,
    )
    return event_data


@router.patch("/{event_data_id}", response_model=EventDataModel)
async def update_event_data(
    request: Request,
    event_data_id: str,
    data: EventDataUpdate,
) -> EventDataModel:
    """
    Update scheduled task
    """
    event_data: EventDataModel = await request.state.controller.update_event_data(
        user=request.state.user,
        persistent=request.state.persistent,
        event_data_id=event_data_id,
        data=data,
    )
    return event_data
