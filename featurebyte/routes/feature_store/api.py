"""
FeatureStore API routes
"""
from __future__ import annotations

from typing import Optional, cast

from http import HTTPStatus

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
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
    VerboseQuery,
)
from featurebyte.schema.feature_store import FeatureStoreCreate, FeatureStoreInfo, FeatureStoreList

router = APIRouter(prefix="/feature_store")


@router.post("", response_model=FeatureStoreModel, status_code=HTTPStatus.CREATED)
async def create_feature_store(request: Request, data: FeatureStoreCreate) -> FeatureStoreModel:
    """
    Create Feature Store
    """
    controller = request.state.app_container.feature_store_controller
    feature_store: FeatureStoreModel = await controller.create_feature_store(data=data)
    return feature_store


@router.get("/{feature_store_id}", response_model=FeatureStoreModel)
async def get_feature_store(
    request: Request, feature_store_id: PydanticObjectId
) -> FeatureStoreModel:
    """
    Retrieve Feature Store
    """
    controller = request.state.app_container.feature_store_controller
    feature_store: FeatureStoreModel = await controller.get(
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
    controller = request.state.app_container.feature_store_controller
    feature_store_list: FeatureStoreList = await controller.list(
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
    controller = request.state.app_container.feature_store_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_store_id,
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_store_id}/info", response_model=FeatureStoreInfo)
async def get_feature_store_info(
    request: Request,
    feature_store_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureStoreInfo:
    """
    Retrieve FeatureStore info
    """
    controller = request.state.app_container.feature_store_controller
    info = await controller.get_info(
        document_id=feature_store_id,
        verbose=verbose,
    )
    return cast(FeatureStoreInfo, info)
