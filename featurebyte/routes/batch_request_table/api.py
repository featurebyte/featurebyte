"""
BatchRequestTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request
from starlette.responses import StreamingResponse

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
    VerboseQuery,
)
from featurebyte.schema.batch_request_table import BatchRequestTableCreate, BatchRequestTableList
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.info import BatchRequestTableInfo
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


@router.delete("/{batch_request_table_id}", response_model=Task, status_code=HTTPStatus.ACCEPTED)
async def delete_batch_request_table(
    request: Request, batch_request_table_id: PydanticObjectId
) -> Task:
    """
    Delete BatchRequestTable
    """
    controller = request.state.app_container.batch_request_table_controller
    task_delete: Task = await controller.delete_materialized_table(
        document_id=batch_request_table_id
    )
    return task_delete


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


@router.get("/{batch_request_table_id}/info", response_model=BatchRequestTableInfo)
async def get_batch_request_table_info(
    request: Request, batch_request_table_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> BatchRequestTableInfo:
    """
    Get BatchRequestTable info
    """
    controller = request.state.app_container.batch_request_table_controller
    info = await controller.get_info(document_id=batch_request_table_id, verbose=verbose)
    return cast(BatchRequestTableInfo, info)


@router.get("/pyarrow_table/{batch_request_table_id}")
async def download_table_as_pyarrow_table(
    request: Request, batch_request_table_id: PydanticObjectId
) -> StreamingResponse:
    """
    Download BatchRequestTable as pyarrow table
    """
    controller = request.state.app_container.batch_request_table_controller
    result: StreamingResponse = await controller.download_materialized_table(
        document_id=batch_request_table_id,
        get_credential=request.state.get_credential,
    )
    return result


@router.patch("/{batch_request_table_id}/description", response_model=BatchRequestTableModel)
async def update_batch_request_table_description(
    request: Request,
    batch_request_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> BatchRequestTableModel:
    """
    Update batch_request_table description
    """
    controller = request.state.app_container.batch_request_table_controller
    batch_request_table: BatchRequestTableModel = await controller.update_description(
        document_id=batch_request_table_id,
        description=data.description,
    )
    return batch_request_table
