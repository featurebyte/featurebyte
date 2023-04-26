"""
BatchFeatureTable API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.batch_feature_table import BatchFeatureTableModel
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
from featurebyte.schema.batch_feature_table import (
    BatchFeatureTableCreate,
    BatchFeatureTableInfo,
    BatchFeatureTableList,
)
from featurebyte.schema.task import Task

router = APIRouter(prefix="/batch_feature_table")


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_batch_feature_table(
    request: Request,
    data: BatchFeatureTableCreate,
) -> Task:
    """
    Create BatchFeatureTable by submitting a materialization task
    """
    controller = request.state.app_container.batch_feature_table_controller
    task_submit: Task = await controller.create_batch_feature_table(
        data=data,
    )
    return task_submit


@router.get("/{batch_feature_table_id}", response_model=BatchFeatureTableModel)
async def get_batch_feature_table(
    request: Request, batch_feature_table_id: PydanticObjectId
) -> BatchFeatureTableModel:
    """
    Get BatchFeatureTable
    """
    controller = request.state.app_container.batch_feature_table_controller
    batch_feature_table: BatchFeatureTableModel = await controller.get(
        document_id=batch_feature_table_id
    )
    return batch_feature_table


@router.get("", response_model=BatchFeatureTableList)
async def list_batch_feature_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> BatchFeatureTableList:
    """
    List BatchFeatureTables
    """
    controller = request.state.app_container.batch_feature_table_controller
    batch_feature_table_list: BatchFeatureTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return batch_feature_table_list


@router.get("/audit/{batch_feature_table_id}", response_model=AuditDocumentList)
async def list_batch_feature_table_audit_logs(
    request: Request,
    batch_feature_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List BatchFeatureTable audit logs
    """
    controller = request.state.app_container.batch_feature_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=batch_feature_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{batch_feature_table_id}/info", response_model=BatchFeatureTableInfo)
async def get_batch_feature_table_info(
    request: Request, batch_feature_table_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> BatchFeatureTableInfo:
    """
    Get BatchFeatureTable info
    """
    controller = request.state.app_container.batch_feature_table_controller
    info = await controller.get_info(document_id=batch_feature_table_id, verbose=verbose)
    return info
