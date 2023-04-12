"""
ModelingTable API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.modeling_table import ModelingTableModel
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
from featurebyte.schema.modeling_table import ModelingTableCreate, ModelingTableList
from featurebyte.schema.task import Task

router = APIRouter(prefix="/modeling_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_modeling_table(
    request: Request,
    data: ModelingTableCreate,
) -> Task:
    """
    Create ModelingTable by submitting a materialization task
    """
    controller = request.state.app_container.modeling_table_controller
    task_submit: Task = await controller.create_modeling_table(
        data=data,
    )
    return task_submit


@router.get("/{modeling_table_id}", response_model=ModelingTableModel)
async def get_modeling_table(
    request: Request, modeling_table_id: PydanticObjectId
) -> ModelingTableModel:
    """
    Get ModelingTable
    """
    controller = request.state.app_container.modeling_table_controller
    modeling_table: ModelingTableModel = await controller.get(document_id=modeling_table_id)
    return modeling_table


@router.get("", response_model=ModelingTableList)
async def list_modeling_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ModelingTableList:
    """
    List ModelingTables
    """
    controller = request.state.app_container.modeling_table_controller
    periodic_task_list: ModelingTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return periodic_task_list


@router.get("/audit/{modeling_table_id}", response_model=AuditDocumentList)
async def list_modeling_table_audit_logs(
    request: Request,
    modeling_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List ModelingTable audit logs
    """
    controller = request.state.app_container.modeling_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=modeling_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list
