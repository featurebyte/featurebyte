"""
EventData API routes
"""
from __future__ import annotations

from typing import List, Optional

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Query, Request

from featurebyte.models.event_data import EventDataModel, FeatureJobSettingHistoryEntry
from featurebyte.models.persistent import AuditDocumentList
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
async def list_event_data(
    request: Request,
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="created_at", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
    name: Optional[str] = Query(default=None, min_length=1, max_length=255),
) -> EventDataList:
    """
    List Event Datas
    """
    event_data_list: EventDataList = await request.state.controller.list(
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
    event_data: EventDataModel = await request.state.controller.get(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=event_data_id,
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


@router.get("/audit/{event_data_id}", response_model=AuditDocumentList)
async def list_event_data_audit_logs(
    request: Request,
    event_data_id: PydanticObjectId,
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="_id", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
) -> AuditDocumentList:
    """
    List Event Data audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=event_data_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get(
    "/history/default_feature_job_setting/{event_data_id}",
    response_model=List[FeatureJobSettingHistoryEntry],
)
async def list_default_feature_job_setting_history(
    request: Request,
    event_data_id: PydanticObjectId,
) -> List[FeatureJobSettingHistoryEntry]:
    """
    List Event Data default feature job settings history
    """
    history_values = await request.state.controller.list_field_history(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=event_data_id,
        field="default_feature_job_setting",
    )

    return [
        FeatureJobSettingHistoryEntry(
            created_at=record.created_at,
            setting=record.value,
        )
        for record in history_values
    ]
