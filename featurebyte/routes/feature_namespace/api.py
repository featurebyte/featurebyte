"""
FeatureNamespace API routes
"""
from __future__ import annotations

from typing import Optional, cast

from beanie import PydanticObjectId
from fastapi import APIRouter, Request

from featurebyte.models.feature import FeatureNamespaceModel
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
from featurebyte.schema.feature_namespace import (
    FeatureNamespaceInfo,
    FeatureNamespaceList,
    FeatureNamespaceUpdate,
)

router = APIRouter(prefix="/feature_namespace")


@router.get("/{feature_namespace_id}", response_model=FeatureNamespaceModel)
async def get_feature_namespace(
    request: Request, feature_namespace_id: PydanticObjectId
) -> FeatureNamespaceModel:
    """
    Retrieve Feature Namespace
    """
    feature_namespace: FeatureNamespaceModel = await request.state.controller.get(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_namespace_id,
        exception_detail=(
            f'FeatureNamespace (id: "{feature_namespace_id}") not found. Please save the Feature object first.'
        ),
    )
    return feature_namespace


@router.patch("/{feature_namespace_id}", response_model=FeatureNamespaceModel)
async def update_feature(
    request: Request, feature_namespace_id: PydanticObjectId, data: FeatureNamespaceUpdate
) -> FeatureNamespaceModel:
    """
    Update FeatureNamespace
    """
    feature: FeatureNamespaceModel = await request.state.controller.update_feature_namespace(
        user=request.state.user,
        persistent=request.state.persistent,
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
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureNamespaceList:
    """
    List FeatureNamespace
    """
    feature_namespace_list: FeatureNamespaceList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
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
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature Namespace audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_namespace_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
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

    info = await request.state.controller.get_info(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_namespace_id,
        verbose=verbose,
    )
    return cast(FeatureNamespaceInfo, info)
