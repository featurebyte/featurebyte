"""
Target API routes
"""
from __future__ import annotations

from typing import Any, Dict, Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Query, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.models.target import TargetModel
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.feature_list import SampleEntityServingNames
from featurebyte.schema.preview import TargetPreview
from featurebyte.schema.target import TargetCreate, TargetInfo, TargetList, TargetUpdate

router = APIRouter(prefix="/target")


class TargetRouter(BaseRouter):
    """
    Target router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=TargetModel, status_code=HTTPStatus.CREATED)
async def create_target(request: Request, data: TargetCreate) -> TargetModel:
    """
    Create target
    """
    controller = request.state.app_container.target_controller
    target: TargetModel = await controller.create_target(data=data)
    return target


@router.get("", response_model=TargetList)
async def list_target(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = SearchQuery,
) -> TargetList:
    """
    List Target's
    """
    controller = request.state.app_container.target_controller
    target_list: TargetList = await controller.list_target(
        page=page, page_size=page_size, sort_by=sort_by, sort_dir=sort_dir, search=search, name=name
    )
    return target_list


@router.get("/{target_id}", response_model=TargetModel)
async def get_target(request: Request, target_id: PydanticObjectId) -> TargetModel:
    """
    Retrieve Target
    """
    controller = request.state.app_container.target_controller
    target: TargetModel = await controller.get(
        document_id=target_id,
    )
    return target


@router.patch("/{target_id}")
async def update_target(
    request: Request,
    target_id: PydanticObjectId,
    data: TargetUpdate,
) -> TargetModel:
    """
    Update Target
    """
    controller = request.state.app_container.target_controller
    target = await controller.target_service.update_document(target_id, data)
    return cast(TargetModel, target)


@router.get("/{target_id}/info", response_model=TargetInfo)
async def get_target_info(
    request: Request,
    target_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> TargetInfo:
    """
    Retrieve target info
    """
    controller = request.state.app_container.target_controller
    info = await controller.get_info(
        document_id=target_id,
        verbose=verbose,
    )
    return cast(TargetInfo, info)


@router.get("/audit/{target_id}", response_model=AuditDocumentList)
async def list_target_audit_logs(
    request: Request,
    target_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List target audit logs
    """
    controller = request.state.app_container.target_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=target_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.post("/preview", response_model=Dict[str, Any])
async def get_target_preview(
    request: Request,
    target_preview: TargetPreview,
) -> Dict[str, Any]:
    """
    Retrieve Target preview
    """
    controller = request.state.app_container.target_controller
    return cast(
        Dict[str, Any],
        await controller.preview(target_preview=target_preview),
    )


@router.patch("/{target_id}/description", response_model=TargetModel)
async def update_target_description(
    request: Request,
    target_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> TargetModel:
    """
    Update target description
    """
    controller = request.state.app_container.target_controller
    target: TargetModel = await controller.update_description(
        document_id=target_id,
        description=data.description,
    )
    return target


@router.get(
    "/{target_id}/sample_entity_serving_names",
    response_model=SampleEntityServingNames,
)
async def get_feature_sample_entity_serving_names(
    request: Request,
    target_id: PydanticObjectId,
    count: int = Query(default=1, gt=0, le=10),
) -> SampleEntityServingNames:
    """
    Get Feature Sample Entity Serving Names
    """
    controller = request.state.app_container.target_controller
    sample_entity_serving_names: SampleEntityServingNames = (
        await controller.get_sample_entity_serving_names(target_id=target_id, count=count)
    )
    return sample_entity_serving_names


@router.delete("/{target_id}")
async def delete_target(request: Request, target_id: PydanticObjectId) -> None:
    """
    Delete Target
    """
    controller = request.state.app_container.target_controller
    await controller.delete(document_id=target_id)
