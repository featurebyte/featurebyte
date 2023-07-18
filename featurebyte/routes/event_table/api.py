"""
EventTable API routes
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.event_table import EventTableModel, FeatureJobSettingHistoryEntry
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
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.event_table import EventTableCreate, EventTableList, EventTableUpdate
from featurebyte.schema.info import EventTableInfo

router = APIRouter(prefix="/event_table")


@router.post("", response_model=EventTableModel, status_code=HTTPStatus.CREATED)
async def create_event_table(request: Request, data: EventTableCreate) -> EventTableModel:
    """
    Create EventTable
    """
    controller = request.state.app_container.event_table_controller
    event_table: EventTableModel = await controller.create_table(data=data)
    return event_table


@router.get("", response_model=EventTableList)
async def list_event_table(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> EventTableList:
    """
    List EventTable
    """
    controller = request.state.app_container.event_table_controller
    event_table_list: EventTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return event_table_list


@router.get("/{event_table_id}", response_model=EventTableModel)
async def get_event_table(request: Request, event_table_id: PydanticObjectId) -> EventTableModel:
    """
    Retrieve EventTable
    """
    controller = request.state.app_container.event_table_controller
    event_table: EventTableModel = await controller.get(
        document_id=event_table_id,
    )
    return event_table


@router.patch("/{event_table_id}", response_model=EventTableModel)
async def update_event_table(
    request: Request,
    event_table_id: PydanticObjectId,
    data: EventTableUpdate,
) -> EventTableModel:
    """
    Update EventTable
    """
    controller = request.state.app_container.event_table_controller
    event_table: EventTableModel = await controller.update_table(
        document_id=event_table_id,
        data=data,
    )
    return event_table


@router.get("/audit/{event_table_id}", response_model=AuditDocumentList)
async def list_event_table_audit_logs(
    request: Request,
    event_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List EventTable audit logs
    """
    controller = request.state.app_container.event_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=event_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get(
    "/history/default_feature_job_setting/{event_table_id}",
    response_model=List[FeatureJobSettingHistoryEntry],
)
async def list_default_feature_job_setting_history(
    request: Request,
    event_table_id: PydanticObjectId,
) -> List[FeatureJobSettingHistoryEntry]:
    """
    List EventTable default feature job settings history
    """
    controller = request.state.app_container.event_table_controller
    history_values = await controller.list_field_history(
        document_id=event_table_id,
        field="default_feature_job_setting",
    )

    return [
        FeatureJobSettingHistoryEntry(
            created_at=record.created_at,
            setting=record.value,
        )
        for record in history_values
    ]


@router.get("/{event_table_id}/info", response_model=EventTableInfo)
async def get_event_table_info(
    request: Request,
    event_table_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> EventTableInfo:
    """
    Retrieve EventTable info
    """
    controller = request.state.app_container.event_table_controller
    info = await controller.get_info(
        document_id=event_table_id,
        verbose=verbose,
    )
    return cast(EventTableInfo, info)


@router.patch("/{event_table_id}/description", response_model=EventTableModel)
async def update_event_table_description(
    request: Request,
    event_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> EventTableModel:
    """
    Update event_table description
    """
    controller = request.state.app_container.event_table_controller
    event_table: EventTableModel = await controller.update_description(
        document_id=event_table_id,
        description=data.description,
    )
    return event_table
