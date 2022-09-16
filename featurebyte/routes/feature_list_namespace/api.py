"""
FeatureListNamespace API routes
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Optional, cast

from beanie import PydanticObjectId
from fastapi import APIRouter, Request

from featurebyte.models.feature_list import FeatureListNamespaceModel
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
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceInfo,
    FeatureListNamespaceList,
    FeatureListNamespaceUpdate,
)

router = APIRouter(prefix="/feature_list_namespace")


@router.get("/{feature_list_namespace_id}", response_model=FeatureListNamespaceModel)
async def get_feature_list_namespace(
    request: Request, feature_list_namespace_id: PydanticObjectId
) -> FeatureListNamespaceModel:
    """
    Get FeatureListNamespace
    """
    feature_list_namespace: FeatureListNamespaceModel = await request.state.controller.get(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_list_namespace_id,
        exception_detail=(
            f'FeatureListNamespace (id: "{feature_list_namespace_id}") not found. '
            "Please save the FeatureList object first."
        ),
    )
    return feature_list_namespace


@router.patch("/{feature_list_namespace_id}", response_model=FeatureListNamespaceModel)
async def update_feature_list_namespace(
    request: Request, feature_list_namespace_id: PydanticObjectId, data: FeatureListNamespaceUpdate
) -> FeatureListNamespaceModel:
    """
    Update FeatureListNamespace
    """
    feature_list_namespace: FeatureListNamespaceModel = (
        await request.state.controller.update_feature_list_namespace(
            user=request.state.user,
            persistent=request.state.persistent,
            feature_list_namespace_id=feature_list_namespace_id,
            data=data,
        )
    )
    return feature_list_namespace


@router.get("", response_model=FeatureListNamespaceList)
async def list_feature_list_namespace(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureListNamespaceList:
    """
    List FeatureListNamespaces
    """
    feature_list_paginated_list: FeatureListNamespaceList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return feature_list_paginated_list


@router.get("/audit/{feature_list_namespace_id}", response_model=AuditDocumentList)
async def list_feature_list_namespace_audit_logs(
    request: Request,
    feature_list_namespace_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List FeatureListNamespace audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_list_namespace_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_list_namespace_id}/info", response_model=FeatureListNamespaceInfo)
async def get_feature_list_namespace_info(
    request: Request,
    feature_list_namespace_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureListNamespaceInfo:
    """
    Retrieve FeatureListNamespace info
    """
    info = await request.state.controller.get_info(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_list_namespace_id,
        verbose=verbose,
    )
    return cast(FeatureListNamespaceInfo, info)
