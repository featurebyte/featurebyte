"""
Feature API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature import FeatureModel
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
from featurebyte.schema.feature import (
    FeatureCreate,
    FeatureInfo,
    FeaturePaginatedList,
    FeaturePreview,
    FeatureUpdate,
)

router = APIRouter(prefix="/feature")


@router.post("", response_model=FeatureModel, status_code=HTTPStatus.CREATED)
async def create_feature(request: Request, data: FeatureCreate) -> FeatureModel:
    """
    Create Feature
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModel = await controller.create_feature(
        get_credential=request.state.get_credential,
        data=data,
    )
    return feature


@router.get("/{feature_id}", response_model=FeatureModel)
async def get_feature(request: Request, feature_id: PydanticObjectId) -> FeatureModel:
    """
    Get Feature
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModel = await controller.get(document_id=feature_id)
    return feature


@router.patch("/{feature_id}", response_model=FeatureModel)
async def update_feature(
    request: Request, feature_id: PydanticObjectId, data: FeatureUpdate
) -> FeatureModel:
    """
    Update Feature
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModel = await controller.update_feature(
        feature_id=feature_id,
        data=data,
    )
    return feature


@router.get("", response_model=FeaturePaginatedList)
async def list_features(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    feature_list_id: Optional[PydanticObjectId] = None,
    feature_namespace_id: Optional[PydanticObjectId] = None,
) -> FeaturePaginatedList:
    """
    List Features
    """
    controller = request.state.app_container.feature_controller
    feature_list: FeaturePaginatedList = await controller.list_features(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
        feature_list_id=feature_list_id,
        feature_namespace_id=feature_namespace_id,
    )
    return feature_list


@router.get("/audit/{feature_id}", response_model=AuditDocumentList)
async def list_feature_audit_logs(
    request: Request,
    feature_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature audit logs
    """
    controller = request.state.app_container.feature_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_id}/info", response_model=FeatureInfo)
async def get_feature_info(
    request: Request,
    feature_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureInfo:
    """
    Retrieve Feature info
    """
    controller = request.state.app_container.feature_controller
    info = await controller.get_info(
        document_id=feature_id,
        verbose=verbose,
    )
    return cast(FeatureInfo, info)


@router.post("/preview", response_model=str)
async def get_feature_preview(
    request: Request,
    feature_preview: FeaturePreview,
) -> str:
    """
    Retrieve Feature preview
    """
    controller = request.state.app_container.feature_controller
    return cast(
        str,
        await controller.preview(
            feature_preview=feature_preview, get_credential=request.state.get_credential
        ),
    )
