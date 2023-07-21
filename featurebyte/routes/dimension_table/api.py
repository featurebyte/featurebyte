"""
DimensionTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.dimension_table import DimensionTableModel
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
from featurebyte.schema.dimension_table import (
    DimensionTableCreate,
    DimensionTableList,
    DimensionTableUpdate,
)
from featurebyte.schema.info import DimensionTableInfo

router = APIRouter(prefix="/dimension_table")


@router.post("", response_model=DimensionTableModel, status_code=HTTPStatus.CREATED)
async def create_dimension_table(
    request: Request, data: DimensionTableCreate
) -> DimensionTableModel:
    """
    Create DimensionTable
    """
    controller = request.state.app_container.dimension_table_controller
    dimension_table: DimensionTableModel = await controller.create_table(data=data)
    return dimension_table


@router.get("", response_model=DimensionTableList)
async def list_dimension_table(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> DimensionTableList:
    """
    List DimensionTable
    """
    controller = request.state.app_container.dimension_table_controller
    dimension_table_list: DimensionTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return dimension_table_list


@router.get("/{dimension_table_id}", response_model=DimensionTableModel)
async def get_dimension_table(
    request: Request, dimension_table_id: PydanticObjectId
) -> DimensionTableModel:
    """
    Retrieve DimensionTable
    """
    controller = request.state.app_container.dimension_table_controller
    dimension_table: DimensionTableModel = await controller.get(
        document_id=dimension_table_id,
    )
    return dimension_table


@router.patch("/{dimension_table_id}", response_model=DimensionTableModel)
async def update_dimension_table(
    request: Request,
    dimension_table_id: PydanticObjectId,
    data: DimensionTableUpdate,
) -> DimensionTableModel:
    """
    Update DimensionTable
    """
    controller = request.state.app_container.dimension_table_controller
    dimension_table: DimensionTableModel = await controller.update_table(
        document_id=dimension_table_id,
        data=data,
    )
    return dimension_table


@router.get("/audit/{dimension_table_id}", response_model=AuditDocumentList)
async def list_dimension_table_audit_logs(
    request: Request,
    dimension_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List DimensionTable audit logs
    """
    controller = request.state.app_container.dimension_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=dimension_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{dimension_table_id}/info", response_model=DimensionTableInfo)
async def get_dimension_table_info(
    request: Request,
    dimension_table_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> DimensionTableInfo:
    """
    Retrieve DimensionTable info
    """
    controller = request.state.app_container.dimension_table_controller
    info = await controller.get_info(
        document_id=dimension_table_id,
        verbose=verbose,
    )
    return cast(DimensionTableInfo, info)


@router.patch("/{dimension_table_id}/description", response_model=DimensionTableModel)
async def update_dimension_table_description(
    request: Request,
    dimension_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> DimensionTableModel:
    """
    Update dimension_table description
    """
    controller = request.state.app_container.dimension_table_controller
    dimension_table: DimensionTableModel = await controller.update_description(
        document_id=dimension_table_id,
        description=data.description,
    )
    return dimension_table
