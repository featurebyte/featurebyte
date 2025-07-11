"""
FeatureNamespace API routes
"""

from __future__ import annotations

from typing import Optional, cast

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
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
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceList,
    FeatureNamespaceModelResponse,
    FeatureNamespaceUpdate,
)
from featurebyte.schema.info import FeatureNamespaceInfo

router = APIRouter(prefix="/feature_namespace")


class FeatureNamespaceRouter(BaseRouter):
    """
    Feature namespace router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.get("/{feature_namespace_id}", response_model=FeatureNamespaceModelResponse)
async def get_feature_namespace(
    request: Request, feature_namespace_id: PydanticObjectId
) -> FeatureNamespaceModelResponse:
    """
    Retrieve Feature Namespace
    """
    controller = request.state.app_container.feature_namespace_controller
    feature_namespace: FeatureNamespaceModelResponse = await controller.get(
        document_id=feature_namespace_id,
        exception_detail=(
            f'FeatureNamespace (id: "{feature_namespace_id}") not found. Please save the Feature object first.'
        ),
    )
    return feature_namespace


@router.patch("/{feature_namespace_id}", response_model=FeatureNamespaceModelResponse)
async def update_feature(
    request: Request, feature_namespace_id: PydanticObjectId, data: FeatureNamespaceUpdate
) -> FeatureNamespaceModelResponse:
    """
    Update FeatureNamespace
    """
    controller = request.state.app_container.feature_namespace_controller
    feature: FeatureNamespaceModelResponse = await controller.update_feature_namespace(
        feature_namespace_id=feature_namespace_id,
        data=data,
    )
    return feature


@router.get("", response_model=FeatureNamespaceList)
async def list_feature_namespaces(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureNamespaceList:
    """
    List FeatureNamespace
    """
    controller = request.state.app_container.feature_namespace_controller
    feature_namespace_list: FeatureNamespaceList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
    )
    return feature_namespace_list


@router.get("/audit/{feature_namespace_id}", response_model=AuditDocumentList)
async def list_feature_namespace_audit_logs(
    request: Request,
    feature_namespace_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature Namespace audit logs
    """
    controller = request.state.app_container.feature_namespace_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_namespace_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_namespace_id}/info", response_model=FeatureNamespaceInfo)
async def get_feature_namespace_info(
    request: Request,
    feature_namespace_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureNamespaceInfo:
    """
    Retrieve FeatureNamespace info
    """
    controller = request.state.app_container.feature_namespace_controller
    info = await controller.get_info(
        document_id=feature_namespace_id,
        verbose=verbose,
    )
    return cast(FeatureNamespaceInfo, info)


@router.patch("/{feature_namespace_id}/description", response_model=FeatureNamespaceModelResponse)
async def update_feature_namespace_description(
    request: Request,
    feature_namespace_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> FeatureNamespaceModelResponse:
    """
    Update feature_namespace description
    """
    controller = request.state.app_container.feature_namespace_controller
    feature_namespace: FeatureNamespaceModelResponse = await controller.update_description(
        document_id=feature_namespace_id,
        description=data.description,
    )
    return feature_namespace
