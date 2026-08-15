"""
Exposure API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Query, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.exposure import ExposureModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
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
from featurebyte.routes.exposure.controller import ExposureController
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.exposure import ExposureCreate, ExposureInfo, ExposureList
from featurebyte.schema.feature_list import SampleEntityServingNames

router = APIRouter(prefix="/exposure")


class ExposureRouter(BaseRouter):
    """
    Exposure router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=ExposureModel, status_code=HTTPStatus.CREATED)
async def create_exposure(request: Request, data: ExposureCreate) -> ExposureModel:
    """
    Create exposure
    """
    controller = request.state.app_container.exposure_controller
    exposure: ExposureModel = await controller.create_exposure(data=data)
    return exposure


@router.get("", response_model=ExposureList)
async def list_exposure(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ExposureList:
    """
    List Exposures
    """
    controller = request.state.app_container.exposure_controller
    exposure_list: ExposureList = await controller.list_exposure(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
    )
    return exposure_list


@router.get("/{exposure_id}", response_model=ExposureModel)
async def get_exposure(request: Request, exposure_id: PydanticObjectId) -> ExposureModel:
    """
    Retrieve Exposure
    """
    controller: ExposureController = request.state.app_container.exposure_controller
    return await controller.get(document_id=ObjectId(exposure_id))


@router.get("/{exposure_id}/info", response_model=ExposureInfo)
async def get_exposure_info(
    request: Request,
    exposure_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> ExposureInfo:
    """
    Retrieve exposure info
    """
    controller: ExposureController = request.state.app_container.exposure_controller
    return await controller.get_info(
        document_id=ObjectId(exposure_id),
        verbose=verbose,
    )


@router.get("/audit/{exposure_id}", response_model=AuditDocumentList)
async def list_exposure_audit_logs(
    request: Request,
    exposure_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List exposure audit logs
    """
    controller = request.state.app_container.exposure_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=exposure_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.patch("/{exposure_id}/description", response_model=ExposureModel)
async def update_exposure_description(
    request: Request,
    exposure_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> ExposureModel:
    """
    Update exposure description
    """
    controller: ExposureController = request.state.app_container.exposure_controller
    return await controller.update_description(
        document_id=ObjectId(exposure_id),
        description=data.description,
    )


@router.get(
    "/{exposure_id}/sample_entity_serving_names",
    response_model=SampleEntityServingNames,
)
async def get_exposure_sample_entity_serving_names(
    request: Request,
    exposure_id: PydanticObjectId,
    count: int = Query(default=1, gt=0, le=10),
) -> SampleEntityServingNames:
    """
    Get Exposure Sample Entity Serving Names
    """
    controller: ExposureController = request.state.app_container.exposure_controller
    return await controller.get_sample_entity_serving_names(
        exposure_id=ObjectId(exposure_id), count=count
    )


@router.delete("/{exposure_id}")
async def delete_exposure(request: Request, exposure_id: PydanticObjectId) -> None:
    """
    Delete Exposure
    """
    controller = request.state.app_container.exposure_controller
    await controller.delete(document_id=ObjectId(exposure_id))
