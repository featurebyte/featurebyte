"""
SCDData API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.scd_data import SCDDataModel
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
from featurebyte.schema.info import SCDDataInfo
from featurebyte.schema.scd_data import SCDDataCreate, SCDDataList, SCDDataUpdate

router = APIRouter(prefix="/scd_data")


@router.post("", response_model=SCDDataModel, status_code=HTTPStatus.CREATED)
async def create_scd_data(request: Request, data: SCDDataCreate) -> SCDDataModel:
    """
    Create SCD Data
    """
    controller = request.state.app_container.scd_data_controller
    scd_data: SCDDataModel = await controller.create_data(data=data)
    return scd_data


@router.get("", response_model=SCDDataList)
async def list_scd_data(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> SCDDataList:
    """
    List SCD Datas
    """
    controller = request.state.app_container.scd_data_controller
    scd_data_list: SCDDataList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return scd_data_list


@router.get("/{scd_data_id}", response_model=SCDDataModel)
async def get_scd_data(request: Request, scd_data_id: PydanticObjectId) -> SCDDataModel:
    """
    Retrieve SCD Data
    """
    controller = request.state.app_container.scd_data_controller
    scd_data: SCDDataModel = await controller.get(
        document_id=scd_data_id,
    )
    return scd_data


@router.patch("/{scd_data_id}", response_model=SCDDataModel)
async def update_scd_data(
    request: Request,
    scd_data_id: PydanticObjectId,
    data: SCDDataUpdate,
) -> SCDDataModel:
    """
    Update SCD Data
    """
    controller = request.state.app_container.scd_data_controller
    scd_data: SCDDataModel = await controller.update_data(
        document_id=scd_data_id,
        data=data,
    )
    return scd_data


@router.get("/audit/{scd_data_id}", response_model=AuditDocumentList)
async def list_scd_data_audit_logs(
    request: Request,
    scd_data_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List SCD Data audit logs
    """
    controller = request.state.app_container.scd_data_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=scd_data_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{item_data_id}/info", response_model=SCDDataInfo)
async def get_item_data_info(
    request: Request,
    item_data_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> SCDDataInfo:
    """
    Retrieve SCDData info
    """
    controller = request.state.app_container.scd_data_controller
    info = await controller.get_info(
        document_id=item_data_id,
        verbose=verbose,
    )
    return cast(SCDDataInfo, info)
