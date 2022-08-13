"""
FeatureList API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Query, Request

from featurebyte.models.feature import FeatureListModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.schema.feature_list import FeatureListCreate, FeatureListPaginatedList

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
async def get_feature_list(request: Request, feature_list_id: str) -> FeatureListModel:
    """
    Get FeatureList
    """
    feature_list: FeatureListModel = await request.state.controller.get(
        user=request.state.user, persistent=request.state.persistent, document_id=feature_list_id
    )
    return feature_list


@router.get("", response_model=FeatureListPaginatedList)
async def list_feature_list(
    request: Request,
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="created_at", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
    name: Optional[str] = Query(default=None, min_length=1, max_length=255),
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
    )
    return feature_list_paginated_list


@router.get("/audit/{feature_list_id}", response_model=AuditDocumentList)
async def list_feature_list_audit_logs(
    request: Request,
    feature_list_id: PydanticObjectId,
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="_id", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
) -> AuditDocumentList:
    """
    List Feature audit logs
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
