"""
Feature API routes
"""
from __future__ import annotations

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.schema.feature import Feature, FeatureCreate

router = APIRouter(prefix="/feature")


@router.post("", response_model=Feature, status_code=HTTPStatus.CREATED)
def create_feature(request: Request, data: FeatureCreate) -> Feature:
    """
    Create Feature
    """
    feature: Feature = request.state.controller.create_feature(
        user=request.state.user,
        persistent=request.state.persistent,
        get_credential=request.state.get_credential,
        data=data,
    )
    return feature
