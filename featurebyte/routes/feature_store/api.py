"""
FeatureStore API routes
"""
from __future__ import annotations

from typing import Any, Dict, Optional, cast

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Request

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.routes.common.schema import (
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreInfo, FeatureStoreList

router = APIRouter(prefix="/feature_store")


@router.post("", response_model=FeatureStoreModel, status_code=HTTPStatus.CREATED)
async def create_feature_store(request: Request, data: FeatureStoreCreate) -> FeatureStoreModel:
    """
    Create Feature Store
    """
    feature_store: FeatureStoreModel = await request.state.controller.create_feature_store(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return feature_store


@router.get("/{feature_store_id}", response_model=FeatureStoreModel)
async def get_feature_store(
    request: Request, feature_store_id: PydanticObjectId
) -> FeatureStoreModel:
    """
    Retrieve Feature Store
    """
    feature_store: FeatureStoreModel = await request.state.controller.get(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_store_id,
    )
    return feature_store


@router.get("", response_model=FeatureStoreList)
async def list_feature_stores(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureStoreList:
    """
    List FeatureStore
    """
    feature_store_list: FeatureStoreList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return feature_store_list


@router.get("/audit/{feature_store_id}", response_model=AuditDocumentList)
async def list_feature_store_audit_logs(
    request: Request,
    feature_store_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature Store audit logs
    """
    audit_doc_list: AuditDocumentList = await request.state.controller.list_audit(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_store_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_store_id}/info")
async def get_feature_store_info(
    request: Request, feature_store_id: PydanticObjectId
) -> FeatureStoreInfo:
    """
    Retrieve FeatureStore info
    """
    info = await request.state.controller.get_info(
        user=request.state.user,
        persistent=request.state.persistent,
        document_id=feature_store_id,
    )
    return info
