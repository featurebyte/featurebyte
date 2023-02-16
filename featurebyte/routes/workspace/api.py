"""
Workspace API routes
"""
from __future__ import annotations

from typing import List, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.workspace import WorkspaceModel, WorkspaceNameHistoryEntry
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
from featurebyte.schema.info import WorkspaceInfo
from featurebyte.schema.workspace import WorkspaceCreate, WorkspaceList, WorkspaceUpdate

router = APIRouter(prefix="/workspace")


@router.post("", response_model=WorkspaceModel, status_code=HTTPStatus.CREATED)
async def create_workspace(request: Request, data: WorkspaceCreate) -> WorkspaceModel:
    """
    Create Workspace
    """
    controller = request.state.app_container.workspace_controller
    workspace: WorkspaceModel = await controller.create_workspace(data=data)
    return workspace


@router.get("/{workspace_id}", response_model=WorkspaceModel)
async def get_workspace(request: Request, workspace_id: PydanticObjectId) -> WorkspaceModel:
    """
    Get Workspace
    """
    controller = request.state.app_container.workspace_controller
    workspace: WorkspaceModel = await controller.get(document_id=workspace_id)
    return workspace


@router.get("", response_model=WorkspaceList)
async def list_workspaces(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> WorkspaceList:
    """
    List Workspace
    """
    controller = request.state.app_container.workspace_controller
    workspace_list: WorkspaceList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return workspace_list


@router.patch("/{workspace_id}", response_model=WorkspaceModel)
async def update_workspace(
    request: Request,
    workspace_id: PydanticObjectId,
    data: WorkspaceUpdate,
) -> WorkspaceModel:
    """
    Update Workspace
    """
    controller = request.state.app_container.workspace_controller
    workspace: WorkspaceModel = await controller.update_workspace(
        workspace_id=workspace_id,
        data=data,
    )
    return workspace


@router.get("/audit/{workspace_id}", response_model=AuditDocumentList)
async def list_workspace_audit_logs(
    request: Request,
    workspace_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Workspace audit logs
    """
    controller = request.state.app_container.workspace_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=workspace_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get(
    "/history/name/{workspace_id}",
    response_model=List[WorkspaceNameHistoryEntry],
)
async def list_name_history(
    request: Request,
    workspace_id: PydanticObjectId,
) -> List[WorkspaceNameHistoryEntry]:
    """
    List Workspace name history
    """
    controller = request.state.app_container.workspace_controller
    history_values = await controller.list_field_history(
        document_id=workspace_id,
        field="name",
    )

    return [
        WorkspaceNameHistoryEntry(
            created_at=record.created_at,
            name=record.value,
        )
        for record in history_values
    ]


@router.get("/{workspace_id}/info", response_model=WorkspaceInfo)
async def get_workspace_info(
    request: Request,
    workspace_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> WorkspaceInfo:
    """
    Retrieve Workspace info
    """
    controller = request.state.app_container.workspace_controller
    info = await controller.get_info(
        document_id=workspace_id,
        verbose=verbose,
    )
    return cast(WorkspaceInfo, info)
