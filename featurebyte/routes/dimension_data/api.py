"""
DimensionData API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.dimension_data import DimensionDataModel
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
from featurebyte.schema.dimension_data import (
    DimensionDataCreate,
    DimensionDataList,
    DimensionDataUpdate,
)
from featurebyte.schema.info import DimensionDataInfo

router = APIRouter(prefix="/dimension_data")


@router.post("", response_model=DimensionDataModel, status_code=HTTPStatus.CREATED)
async def create_dimension_data(request: Request, data: DimensionDataCreate) -> DimensionDataModel:
    """
    Create Dimension Data
    """
    controller = request.state.app_container.dimension_data_controller
    dimension_data: DimensionDataModel = await controller.create_data(data=data)
    return dimension_data


@router.get("", response_model=DimensionDataList)
async def list_dimension_data(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> DimensionDataList:
    """
    List Dimension Datas
    """
    controller = request.state.app_container.dimension_data_controller
    dimension_data_list: DimensionDataList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return dimension_data_list


@router.get("/{dimension_data_id}", response_model=DimensionDataModel)
async def get_dimension_data(
    request: Request, dimension_data_id: PydanticObjectId
) -> DimensionDataModel:
    """
    Retrieve Dimension Data
    """
    controller = request.state.app_container.dimension_data_controller
    dimension_data: DimensionDataModel = await controller.get(
        document_id=dimension_data_id,
    )
    return dimension_data


@router.patch("/{dimension_data_id}", response_model=DimensionDataModel)
async def update_dimension_data(
    request: Request,
    dimension_data_id: PydanticObjectId,
    data: DimensionDataUpdate,
) -> DimensionDataModel:
    """
    Update Dimension Data
    """
    controller = request.state.app_container.dimension_data_controller
    dimension_data: DimensionDataModel = await controller.update_data(
        document_id=dimension_data_id,
        data=data,
    )
    return dimension_data


@router.get("/audit/{dimension_data_id}", response_model=AuditDocumentList)
async def list_dimension_data_audit_logs(
    request: Request,
    dimension_data_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Dimension Data audit logs
    """
    controller = request.state.app_container.dimension_data_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=dimension_data_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{dimension_data_id}/info", response_model=DimensionDataInfo)
async def get_dimension_data_info(
    request: Request,
    dimension_data_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> DimensionDataInfo:
    """
    Retrieve DimensionData info
    """
    controller = request.state.app_container.dimension_data_controller
    info = await controller.get_info(
        document_id=dimension_data_id,
        verbose=verbose,
    )
    return cast(DimensionDataInfo, info)
