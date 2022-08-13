"""
Feature API routes
"""
from __future__ import annotations

from typing import Optional

from http import HTTPStatus

from beanie import PydanticObjectId
from fastapi import APIRouter, Query, Request

from featurebyte.models.feature import FeatureModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.schema.feature import FeatureCreate, FeatureList

router = APIRouter(prefix="/feature")


@router.post("", response_model=FeatureModel, status_code=HTTPStatus.CREATED)
async def create_feature(request: Request, data: FeatureCreate) -> FeatureModel:
    """
    Create Feature
    """
    feature: FeatureModel = await request.state.controller.create_feature(
        user=request.state.user,
        persistent=request.state.persistent,
        get_credential=request.state.get_credential,
        data=data,
    )
    return feature


@router.get("/{feature_id}", response_model=FeatureModel)
async def get_feature(request: Request, feature_id: str) -> FeatureModel:
    """
    Get Feature
    """
    feature: FeatureModel = await request.state.controller.get(
        user=request.state.user, persistent=request.state.persistent, document_id=feature_id
    )
    return feature


@router.get("", response_model=FeatureList)
async def list_features(
    request: Request,
    page: int = Query(default=1, gt=0),
    page_size: int = Query(default=10, gt=0),
    sort_by: Optional[str] = Query(default="created_at", min_length=1, max_length=255),
    sort_dir: Optional[str] = Query(default="desc", regex="^(asc|desc)$"),
    search: Optional[str] = Query(default=None, min_length=1, max_length=255),
    name: Optional[str] = Query(default=None, min_length=1, max_length=255),
) -> FeatureList:
    """
    List Features
    """
    feature_list: FeatureList = await request.state.controller.list(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return feature_list


@router.get("/audit/{feature_id}", response_model=AuditDocumentList)
async def list_feature_audit_logs(
    request: Request,
    feature_id: PydanticObjectId,
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
        document_id=feature_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list
