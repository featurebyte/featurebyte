"""
Feature API routes
"""
from __future__ import annotations

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.feature import FeatureModel
from featurebyte.schema.feature import FeatureCreate

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
    feature: FeatureModel = await request.state.controller.get_feature(
        user=request.state.user, persistent=request.state.persistent, feature_id=feature_id
    )
    return feature
