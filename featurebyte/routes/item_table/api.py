"""
ItemTable API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_table import ItemTableModel
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
from featurebyte.schema.info import ItemTableInfo
from featurebyte.schema.item_table import ItemTableCreate, ItemTableList, ItemTableUpdate

router = APIRouter(prefix="/item_table")


@router.post("", response_model=ItemTableModel, status_code=HTTPStatus.CREATED)
async def create_item_table(request: Request, data: ItemTableCreate) -> ItemTableModel:
    """
    Create ItemTable
    """
    controller = request.state.app_container.item_table_controller
    item_table: ItemTableModel = await controller.create_table(data=data)
    return item_table


@router.get("", response_model=ItemTableList)
async def list_item_table(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ItemTableList:
    """
    List ItemTable
    """
    controller = request.state.app_container.item_table_controller
    item_table_list: ItemTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return item_table_list


@router.get("/{item_table_id}", response_model=ItemTableModel)
async def get_item_table(request: Request, item_table_id: PydanticObjectId) -> ItemTableModel:
    """
    Retrieve ItemTable
    """
    controller = request.state.app_container.item_table_controller
    item_table: ItemTableModel = await controller.get(
        document_id=item_table_id,
    )
    return item_table


@router.patch("/{item_table_id}", response_model=ItemTableModel)
async def update_item_table(
    request: Request,
    item_table_id: PydanticObjectId,
    data: ItemTableUpdate,
) -> ItemTableModel:
    """
    Update ItemTable
    """
    controller = request.state.app_container.item_table_controller
    item_table: ItemTableModel = await controller.update_table(
        document_id=item_table_id,
        data=data,
    )
    return item_table


@router.get("/audit/{item_table_id}", response_model=AuditDocumentList)
async def list_item_table_audit_logs(
    request: Request,
    item_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List ItemTable audit logs
    """
    controller = request.state.app_container.item_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=item_table_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{item_table_id}/info", response_model=ItemTableInfo)
async def get_item_table_info(
    request: Request,
    item_table_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> ItemTableInfo:
    """
    Retrieve ItemTable info
    """
    controller = request.state.app_container.item_table_controller
    info = await controller.get_info(
        document_id=item_table_id,
        verbose=verbose,
    )
    return cast(ItemTableInfo, info)


@router.patch("/{item_table_id}/description", response_model=ItemTableModel)
async def update_item_table_description(
    request: Request,
    item_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> ItemTableModel:
    """
    Update item_table description
    """
    controller = request.state.app_container.item_table_controller
    item_table: ItemTableModel = await controller.update_description(
        document_id=item_table_id,
        description=data.description,
    )
    return item_table
