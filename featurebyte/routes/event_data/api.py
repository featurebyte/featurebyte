"""
EventData API routes
"""
# pylint: disable=too-few-public-methods,relative-beyond-top-level
from __future__ import annotations

from typing import Literal, Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from .schema import EventData, EventDataCreate, EventDataList, EventDataUpdate

router = APIRouter(prefix="/event_data")


@router.post("", response_model=EventData, status_code=HTTPStatus.CREATED)
def create_event_data(
    request: Request,
    data: EventDataCreate,
) -> EventData:
    """
    Create Event Data
    """
    event_data: EventData = request.state.controller.create_event_data(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return event_data


@router.get("", response_model=EventDataList)
def list_event_datas(
    request: Request,
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    search: Optional[str] = None,
) -> EventDataList:
    """
    List Event Datas
    """
    event_data_list: EventDataList = request.state.controller.list_event_datas(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return event_data_list


@router.get("/{event_data_name}", response_model=EventData)
def retrieve_event_data(
    request: Request,
    event_data_name: str,
) -> Optional[EventData]:
    """
    Retrieve Event Data
    """
    event_data: Optional[EventData] = request.state.controller.retrieve_event_data(
        user=request.state.user,
        persistent=request.state.persistent,
        event_data_name=event_data_name,
    )
    return event_data


@router.patch("/{event_data_name}", response_model=EventData)
def update_event_data(
    request: Request,
    event_data_name: str,
    data: EventDataUpdate,
) -> EventData:
    """
    Update scheduled task
    """
    event_data: EventData = request.state.controller.update_event_data(
        user=request.state.user,
        persistent=request.state.persistent,
        event_data_name=event_data_name,
        data=data,
    )
    return event_data
