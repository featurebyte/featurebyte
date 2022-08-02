"""
Feature API routes
"""
from __future__ import annotations

from typing import Literal, Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.feature import FeatureModel
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
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    name: Optional[str] = None,
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
        name=name,
    )
    return feature_list
