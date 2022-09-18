"""
FeatureList API routes
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
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
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListInfo,
    FeatureListPaginatedList,
    FeatureListUpdate,
)

router = APIRouter(prefix="/feature_list")


@router.post("", response_model=FeatureListModel, status_code=HTTPStatus.CREATED)
async def create_feature_list(request: Request, data: FeatureListCreate) -> FeatureListModel:
    """
    Create FeatureList
    """
    feature_list: FeatureListModel = await request.state.controller.create_feature_list(
        user=request.state.user,
        persistent=request.state.persistent,
        get_credential=request.state.get_credential,
        data=data,
    )
    return feature_list


@router.get("/{feature_list_id}", response_model=FeatureListModel)
async def get_feature_list(request: Request, feature_list_id: PydanticObjectId) -> FeatureListModel:
    """
    Get FeatureList
    """
    feature_list: FeatureListModel = await request.state.controller.get(
        user=request.state.user, persistent=request.state.persistent, document_id=feature_list_id
    )
    return feature_list


@router.patch("/{feature_list_id}", response_model=FeatureListModel)
async def update_feature_list(
    request: Request, feature_list_id: PydanticObjectId, data: FeatureListUpdate
) -> FeatureListModel:
    """
    Update FeatureList
    """
    feature_list: FeatureListModel = await request.state.controller.update_feature_list(
        user=request.state.user,
        persistent=request.state.persistent,
        feature_list_id=feature_list_id,
        data=data,
    )
    return feature_list


@router.get("", response_model=FeatureListPaginatedList)
async def list_feature_list(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    feature_list_namespace_id: Optional[PydanticObjectId] = None,
) -> FeatureListPaginatedList:
    """
    List FeatureLists
    """
    feature_list_paginated_list: FeatureListPaginatedList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
        feature_list_namespace_id=feature_list_namespace_id,
    )
    return feature_list_paginated_list


@router.get("/audit/{feature_list_id}", response_model=AuditDocumentList)
async def list_feature_list_audit_logs(
    request: Request,
    feature_list_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List FeatureList audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_list_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_list_id}/info", response_model=FeatureListInfo)
async def get_feature_list_info(
    request: Request,
    feature_list_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureListInfo:
    """
    Retrieve FeatureList info
    """
    info = await request.state.controller.get_info(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_list_id,
        verbose=verbose,
    )
    return cast(FeatureListInfo, info)
