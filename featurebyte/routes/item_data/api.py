"""
ItemData API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.item_data import ItemDataModel
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
from featurebyte.schema.info import ItemDataInfo
from featurebyte.schema.item_data import ItemDataCreate, ItemDataList, ItemDataUpdate

router = APIRouter(prefix="/item_data")


@router.post("", response_model=ItemDataModel, status_code=HTTPStatus.CREATED)
async def create_item_data(request: Request, data: ItemDataCreate) -> ItemDataModel:
    """
    Create Item Data
    """
    controller = request.state.app_container.item_data_controller
    item_data: ItemDataModel = await controller.create_data(data=data)
    return item_data


@router.get("", response_model=ItemDataList)
async def list_item_data(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ItemDataList:
    """
    List Item Datas
    """
    controller = request.state.app_container.item_data_controller
    item_data_list: ItemDataList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return item_data_list


@router.get("/{item_data_id}", response_model=ItemDataModel)
async def get_item_data(request: Request, item_data_id: PydanticObjectId) -> ItemDataModel:
    """
    Retrieve Item Data
    """
    controller = request.state.app_container.item_data_controller
    item_data: ItemDataModel = await controller.get(
        document_id=item_data_id,
    )
    return item_data


@router.patch("/{item_data_id}", response_model=ItemDataModel)
async def update_item_data(
    request: Request,
    item_data_id: PydanticObjectId,
    data: ItemDataUpdate,
) -> ItemDataModel:
    """
    Update Item Data
    """
    controller = request.state.app_container.item_data_controller
    item_data: ItemDataModel = await controller.update_data(
        document_id=item_data_id,
        data=data,
    )
    return item_data


@router.get("/audit/{item_data_id}", response_model=AuditDocumentList)
async def list_item_data_audit_logs(
    request: Request,
    item_data_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Item Data audit logs
    """
    controller = request.state.app_container.item_data_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=item_data_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{item_data_id}/info", response_model=ItemDataInfo)
async def get_item_data_info(
    request: Request,
    item_data_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> ItemDataInfo:
    """
    Retrieve ItemData info
    """
    controller = request.state.app_container.item_data_controller
    info = await controller.get_info(
        document_id=item_data_id,
        verbose=verbose,
    )
    return cast(ItemDataInfo, info)
