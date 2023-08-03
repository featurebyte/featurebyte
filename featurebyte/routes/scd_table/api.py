"""
SCDTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.scd_table import SCDTableModel
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
from featurebyte.schema.info import SCDTableInfo
from featurebyte.schema.scd_table import SCDTableCreate, SCDTableList, SCDTableUpdate
from featurebyte.schema.table import (
    ColumnCriticalDataInfoUpdate,
    ColumnDescriptionUpdate,
    ColumnEntityUpdate,
    ColumnSemanticUpdate,
)

router = APIRouter(prefix="/scd_table")


@router.post("", response_model=SCDTableModel, status_code=HTTPStatus.CREATED)
async def create_scd_table(request: Request, data: SCDTableCreate) -> SCDTableModel:
    """
    Create SCDTable
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.create_table(data=data)
    return scd_table


@router.get("", response_model=SCDTableList)
async def list_scd_table(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> SCDTableList:
    """
    List SCDTable
    """
    controller = request.state.app_container.scd_table_controller
    scd_table_list: SCDTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return scd_table_list


@router.get("/{scd_table_id}", response_model=SCDTableModel)
async def get_scd_table(request: Request, scd_table_id: PydanticObjectId) -> SCDTableModel:
    """
    Retrieve SCDTable
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.get(
        document_id=scd_table_id,
    )
    return scd_table


@router.patch("/{scd_table_id}", response_model=SCDTableModel)
async def update_scd_table(
    request: Request,
    scd_table_id: PydanticObjectId,
    data: SCDTableUpdate,
) -> SCDTableModel:
    """
    Update SCDTable
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.update_table(
        document_id=scd_table_id,
        data=data,
    )
    return scd_table


@router.get("/audit/{scd_table_id}", response_model=AuditDocumentList)
async def list_scd_table_audit_logs(
    request: Request,
    scd_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List SCDTable audit logs
    """
    controller = request.state.app_container.scd_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=scd_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{scd_table_id}/info", response_model=SCDTableInfo)
async def get_scd_table_info(
    request: Request,
    scd_table_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> SCDTableInfo:
    """
    Retrieve SCDTable info
    """
    controller = request.state.app_container.scd_table_controller
    info = await controller.get_info(
        document_id=scd_table_id,
        verbose=verbose,
    )
    return cast(SCDTableInfo, info)


@router.patch("/{scd_table_id}/description", response_model=SCDTableModel)
async def update_scd_table_description(
    request: Request,
    scd_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> SCDTableModel:
    """
    Update scd_table description
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.update_description(
        document_id=scd_table_id,
        description=data.description,
    )
    return scd_table


@router.patch("/{scd_table_id}/column_entity", response_model=SCDTableModel)
async def update_column_entity(
    request: Request,
    scd_table_id: PydanticObjectId,
    data: ColumnEntityUpdate,
) -> SCDTableModel:
    """
    Update column entity
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.update_column_entity(
        document_id=scd_table_id,
        column_name=data.column_name,
        entity_id=data.entity_id,
    )
    return scd_table


@router.patch("/{scd_table_id}/column_critical_data_info", response_model=SCDTableModel)
async def update_column_critical_data_info(
    request: Request,
    scd_table_id: PydanticObjectId,
    data: ColumnCriticalDataInfoUpdate,
) -> SCDTableModel:
    """
    Update column critical data info
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.update_column_critical_data_info(
        document_id=scd_table_id,
        column_name=data.column_name,
        critical_data_info=data.critical_data_info,
    )
    return scd_table


@router.patch("/{scd_table_id}/column_description", response_model=SCDTableModel)
async def update_column_description(
    request: Request,
    scd_table_id: PydanticObjectId,
    data: ColumnDescriptionUpdate,
) -> SCDTableModel:
    """
    Update column description
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.update_column_description(
        document_id=scd_table_id,
        column_name=data.column_name,
        description=data.description,
    )
    return scd_table


@router.patch("/{scd_table_id}/column_semantic", response_model=SCDTableModel)
async def update_column_semantic(
    request: Request,
    scd_table_id: PydanticObjectId,
    data: ColumnSemanticUpdate,
) -> SCDTableModel:
    """
    Update column semantic
    """
    controller = request.state.app_container.scd_table_controller
    scd_table: SCDTableModel = await controller.update_column_semantic(
        document_id=scd_table_id,
        column_name=data.column_name,
        semantic_id=data.semantic_id,
    )
    return scd_table
