"""
FeatureStore API routes
"""
from __future__ import annotations

from typing import Literal, Optional

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.schema.feature_store import FeatureStore, FeatureStoreCreate, FeatureStoreList

router = APIRouter(prefix="/feature_store")


@router.post("", response_model=FeatureStore, status_code=HTTPStatus.CREATED)
async def create_feature_store(
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
async def get_feature_store(
    request: Request,
    feature_store_id: str,
) -> FeatureStore:
    """
    Retrieve Feature Store
    """
    feature_store: FeatureStore = await request.state.controller.get_feature_store(
        user=request.state.user,
        persistent=request.state.persistent,
        feature_store_id=feature_store_id,
    )
    return feature_store


@router.get("", response_model=FeatureStoreList)
async def list_feature_stores(
    request: Request,
    page: int = 1,
    page_size: int = 10,
    sort_by: Optional[str] = "created_at",
    sort_dir: Literal["asc", "desc"] = "desc",
    name: Optional[str] = None,
) -> FeatureStoreList:
    """
    List FeatureStore
    """
    feature_store_list: FeatureStoreList = await request.state.controller.list_feature_stores(
        user=request.state.user,
        persistent=request.state.persistent,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        name=name,
    )
    return feature_store_list
