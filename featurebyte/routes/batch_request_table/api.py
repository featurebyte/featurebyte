"""
BatchRequestTable API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.batch_request_table import BatchRequestTableModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.batch_request_table import BatchRequestTableCreate, BatchRequestTableList
from featurebyte.schema.task import Task

router = APIRouter(prefix="/batch_request_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_batch_request_table(
    request: Request,
    data: BatchRequestTableCreate,
) -> Task:
    """
    Create BatchRequestTable by submitting a materialization task
    """
    controller = request.state.app_container.batch_request_table_controller
    task_submit: Task = await controller.create_batch_request_table(
        data=data,
    )
    return task_submit


@router.get("/{batch_request_table_id}", response_model=BatchRequestTableModel)
async def get_batch_request_table(
    request: Request, batch_request_table_id: PydanticObjectId
) -> BatchRequestTableModel:
    """
    Get BatchRequestTable
    """
    controller = request.state.app_container.batch_request_table_controller
    batch_request_table: BatchRequestTableModel = await controller.get(
        document_id=batch_request_table_id
    )
    return batch_request_table


@router.get("", response_model=BatchRequestTableList)
async def list_batch_request_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> BatchRequestTableList:
    """
    List BatchRequestTables
    """
    controller = request.state.app_container.batch_request_table_controller
    batch_request_table_list: BatchRequestTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return batch_request_table_list


@router.get("/audit/{batch_request_table_id}", response_model=AuditDocumentList)
async def list_batch_request_table_audit_logs(
    request: Request,
    batch_request_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List BatchRequestTable audit logs
    """
    controller = request.state.app_container.batch_request_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=batch_request_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list
