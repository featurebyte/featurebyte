"""
FeatureStore API routes
"""
from __future__ import annotations

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.schema.feature_store import FeatureStore, FeatureStoreCreate

router = APIRouter(prefix="/feature_store")


@router.post("", response_model=FeatureStore, status_code=HTTPStatus.CREATED)
async def create_event_data(
    request: Request,
    data: FeatureStoreCreate,
) -> FeatureStore:
    """
    Create Feature Store
    """
    feature_store: FeatureStore = await request.state.controller.create_feature_store(
        user=request.state.user, persistent=request.state.persistent, data=data
    )
    return feature_store


@router.get("/{feature_store_id}", response_model=FeatureStore)
async def retrieve_event_data(
    request: Request,
    feature_store_id: str,
) -> FeatureStore:
    """
    Retrieve Feature Store
    """
    feature_store: FeatureStore = await request.state.controller.retrieve_feature_store(
        user=request.state.user,
        persistent=request.state.persistent,
        feature_store_id=feature_store_id,
    )
    return feature_store
