"""
FeatureStore API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Query, Request

from featurebyte.models.feature_store import FeatureStoreModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreList

router = APIRouter(prefix="/feature_store")


@router.post("", response_model=FeatureStoreModel, status_code=HTTPStatus.CREATED)
async def create_feature_store(
    request: Request,
    data: FeatureStoreCreate,
) -> FeatureStoreModel:
    """
    Create Feature Store
    """
    feature_store: FeatureStoreModel = await request.state.controller.create_feature_store(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return feature_store


@router.get("/{feature_store_id}", response_model=FeatureStoreModel)
async def get_feature_store(
    request: Request,
    feature_store_id: str,
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
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="created_at", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
    name: Optional[str] = Query(default=None, min_length=1, max_length=255),
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
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="_id", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
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
