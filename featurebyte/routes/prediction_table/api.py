"""
PredictionTable API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.prediction_table import PredictionTableModel
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.prediction_table import PredictionTableCreate, PredictionTableList
from featurebyte.schema.task import Task

router = APIRouter(prefix="/prediction_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_prediction_table(
    request: Request,
    data: PredictionTableCreate,
) -> Task:
    """
    Create PredictionTable by submitting a materialization task
    """
    controller = request.state.app_container.prediction_table_controller
    task_submit: Task = await controller.create_prediction_table(
        data=data,
    )
    return task_submit


@router.get("/{prediction_table_id}", response_model=PredictionTableModel)
async def get_prediction_table(
    request: Request, prediction_table_id: PydanticObjectId
) -> PredictionTableModel:
    """
    Get PredictionTable
    """
    controller = request.state.app_container.prediction_table_controller
    prediction_table: PredictionTableModel = await controller.get(document_id=prediction_table_id)
    return prediction_table


@router.get("", response_model=PredictionTableList)
async def list_prediction_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> PredictionTableList:
    """
    List PredictionTables
    """
    controller = request.state.app_container.prediction_table_controller
    prediction_table_list: PredictionTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return prediction_table_list


@router.get("/audit/{prediction_table_id}", response_model=AuditDocumentList)
async def list_prediction_table_audit_logs(
    request: Request,
    prediction_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List PredictionTable audit logs
    """
    controller = request.state.app_container.prediction_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=prediction_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list
