"""
EventData API routes
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_data import EventDataModel, FeatureJobSettingHistoryEntry
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.event_data import EventDataCreate, EventDataList, EventDataUpdate
from featurebyte.schema.info import EventDataInfo

router = APIRouter(prefix="/event_data")


@router.post("", response_model=EventDataModel, status_code=HTTPStatus.CREATED)
async def create_event_data(request: Request, data: EventDataCreate) -> EventDataModel:
    """
    Create Event Data
    """
    controller = request.state.app_container.event_data_controller
    event_data: EventDataModel = await controller.create_data(data=data)
    return event_data


@router.get("", response_model=EventDataList)
async def list_event_data(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> EventDataList:
    """
    List Event Datas
    """
    controller = request.state.app_container.event_data_controller
    event_data_list: EventDataList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return event_data_list


@router.get("/{event_data_id}", response_model=EventDataModel)
async def get_event_data(request: Request, event_data_id: PydanticObjectId) -> EventDataModel:
    """
    Retrieve Event Data
    """
    controller = request.state.app_container.event_data_controller
    event_data: EventDataModel = await controller.get(
        document_id=event_data_id,
    )
    return event_data


@router.patch("/{event_data_id}", response_model=EventDataModel)
async def update_event_data(
    request: Request,
    event_data_id: PydanticObjectId,
    data: EventDataUpdate,
) -> EventDataModel:
    """
    Update Event Data
    """
    controller = request.state.app_container.event_data_controller
    event_data: EventDataModel = await controller.update_data(
        document_id=event_data_id,
        data=data,
    )
    return event_data


@router.get("/audit/{event_data_id}", response_model=AuditDocumentList)
async def list_event_data_audit_logs(
    request: Request,
    event_data_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Event Data audit logs
    """
    controller = request.state.app_container.event_data_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
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
    controller = request.state.app_container.event_data_controller
    history_values = await controller.list_field_history(
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


@router.get("/{event_data_id}/info", response_model=EventDataInfo)
async def get_event_data_info(
    request: Request,
    event_data_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> EventDataInfo:
    """
    Retrieve EventData info
    """
    controller = request.state.app_container.event_data_controller
    info = await controller.get_info(
        document_id=event_data_id,
        verbose=verbose,
    )
    return cast(EventDataInfo, info)
