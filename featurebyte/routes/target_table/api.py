"""
TargetTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

import json
from http import HTTPStatus

from fastapi import APIRouter, Form, Request, UploadFile
from starlette.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.target_table import TargetTableModel
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
from featurebyte.schema.info import TargetTableInfo
from featurebyte.schema.target_table import TargetTableCreate, TargetTableList
from featurebyte.schema.task import Task

router = APIRouter(prefix="/target_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_target_table(
    request: Request,
    payload: str = Form(),
    observation_set: Optional[UploadFile] = None,
) -> Task:
    """
    Create TargetTable by submitting a materialization task
    """
    data = TargetTableCreate(**json.loads(payload))
    controller = request.state.app_container.target_table_controller
    task_submit: Task = await controller.create_table(
        data=data,
        observation_set=observation_set,
    )
    return task_submit


@router.get("/{target_table_id}", response_model=TargetTableModel)
async def get_target_table(request: Request, target_table_id: PydanticObjectId) -> TargetTableModel:
    """
    Get TargetTable
    """
    controller = request.state.app_container.target_table_controller
    target_table: TargetTableModel = await controller.get(document_id=target_table_id)
    return target_table


@router.delete("/{target_table_id}", response_model=Task, status_code=HTTPStatus.ACCEPTED)
async def delete_target_table(request: Request, target_table_id: PydanticObjectId) -> Task:
    """
    Delete TargetTable
    """
    controller = request.state.app_container.target_table_controller
    task: Task = await controller.delete_materialized_table(document_id=target_table_id)
    return task


@router.get("", response_model=TargetTableList)
async def list_target_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> TargetTableList:
    """
    List TargetTables
    """
    controller = request.state.app_container.target_table_controller
    target_table_list: TargetTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return target_table_list


@router.get("/audit/{target_table_id}", response_model=AuditDocumentList)
async def list_target_table_audit_logs(
    request: Request,
    target_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List TargetTable audit logs
    """
    controller = request.state.app_container.target_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=target_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{target_table_id}/info", response_model=TargetTableInfo)
async def get_target_table_info(
    request: Request, target_table_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> TargetTableInfo:
    """
    Get TargetTable info
    """
    controller = request.state.app_container.target_table_controller
    info = await controller.get_info(document_id=target_table_id, verbose=verbose)
    return cast(TargetTableInfo, info)


@router.get("/pyarrow_table/{target_table_id}")
async def download_table_as_pyarrow_table(
    request: Request, target_table_id: PydanticObjectId
) -> StreamingResponse:
    """
    Download TargetTable as pyarrow table
    """
    controller = request.state.app_container.target_table_controller
    result: StreamingResponse = await controller.download_materialized_table(
        document_id=target_table_id,
        get_credential=request.state.get_credential,
    )
    return result


@router.patch("/{target_table_id}/description", response_model=TargetTableModel)
async def update_target_table_description(
    request: Request,
    target_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> TargetTableModel:
    """
    Update target_table description
    """
    controller = request.state.app_container.target_table_controller
    target_table: TargetTableModel = await controller.update_description(
        document_id=target_table_id,
        description=data.description,
    )
    return target_table
