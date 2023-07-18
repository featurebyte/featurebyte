"""
StaticSourceTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.static_source_table import StaticSourceTableModel
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
from featurebyte.schema.info import StaticSourceTableInfo
from featurebyte.schema.static_source_table import StaticSourceTableCreate, StaticSourceTableList
from featurebyte.schema.task import Task

router = APIRouter(prefix="/static_source_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_static_source_table(
    request: Request,
    data: StaticSourceTableCreate,
) -> Task:
    """
    Create StaticSourceTable by submitting a materialization task
    """
    controller = request.state.app_container.static_source_table_controller
    task_submit: Task = await controller.create_static_source_table(
        data=data,
    )
    return task_submit


@router.get("/{static_source_table_id}", response_model=StaticSourceTableModel)
async def get_static_source_table(
    request: Request, static_source_table_id: PydanticObjectId
) -> StaticSourceTableModel:
    """
    Get StaticSourceTable
    """
    controller = request.state.app_container.static_source_table_controller
    static_source_table: StaticSourceTableModel = await controller.get(
        document_id=static_source_table_id
    )
    return static_source_table


@router.delete("/{static_source_table_id}", response_model=Task, status_code=HTTPStatus.ACCEPTED)
async def delete_static_source_table(
    request: Request, static_source_table_id: PydanticObjectId
) -> Task:
    """
    Delete StaticSourceTable by submitting a deletion task
    """
    controller = request.state.app_container.static_source_table_controller
    task: Task = await controller.delete_materialized_table(document_id=static_source_table_id)
    return task


@router.get("", response_model=StaticSourceTableList)
async def list_static_source_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> StaticSourceTableList:
    """
    List StaticSourceTables
    """
    controller = request.state.app_container.static_source_table_controller
    static_source_table_list: StaticSourceTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return static_source_table_list


@router.get("/audit/{static_source_table_id}", response_model=AuditDocumentList)
async def list_static_source_table_audit_logs(
    request: Request,
    static_source_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List StaticSourceTable audit logs
    """
    controller = request.state.app_container.static_source_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=static_source_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{static_source_table_id}/info", response_model=StaticSourceTableInfo)
async def get_static_source_table_info(
    request: Request, static_source_table_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> StaticSourceTableInfo:
    """
    Get StaticSourceTable info
    """
    controller = request.state.app_container.static_source_table_controller
    info = await controller.get_info(document_id=static_source_table_id, verbose=verbose)
    return cast(StaticSourceTableInfo, info)


@router.get("/pyarrow_table/{static_source_table_id}")
async def download_table_as_pyarrow_table(
    request: Request, static_source_table_id: PydanticObjectId
) -> StreamingResponse:
    """
    Download StaticSourceTable as pyarrow table
    """
    controller = request.state.app_container.static_source_table_controller
    result: StreamingResponse = await controller.download_materialized_table(
        document_id=static_source_table_id,
        get_credential=request.state.get_credential,
    )
    return result


@router.patch("/{static_source_table_id}/description", response_model=StaticSourceTableModel)
async def update_static_source_table_description(
    request: Request,
    static_source_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> StaticSourceTableModel:
    """
    Update static_source_table description
    """
    controller = request.state.app_container.static_source_table_controller
    static_source_table: StaticSourceTableModel = await controller.update_description(
        document_id=static_source_table_id,
        description=data.description,
    )
    return static_source_table
