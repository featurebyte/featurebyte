"""
FeatureListNamespace API routes
"""

from __future__ import annotations

from typing import Optional, cast

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
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
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.feature_list_namespace import (
    FeatureListNamespaceList,
    FeatureListNamespaceModelResponse,
    FeatureListNamespaceUpdate,
)
from featurebyte.schema.info import FeatureListNamespaceInfo

router = APIRouter(prefix="/feature_list_namespace")


class FeatureListNamespaceRouter(BaseRouter):
    """
    Feature list namespace router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.get("/{feature_list_namespace_id}", response_model=FeatureListNamespaceModelResponse)
async def get_feature_list_namespace(
    request: Request, feature_list_namespace_id: PydanticObjectId
) -> FeatureListNamespaceModelResponse:
    """
    Get FeatureListNamespace
    """
    controller = request.state.app_container.feature_list_namespace_controller
    feature_list_namespace: FeatureListNamespaceModelResponse = await controller.get(
        document_id=feature_list_namespace_id,
        exception_detail=(
            f'FeatureListNamespace (id: "{feature_list_namespace_id}") not found. '
            "Please save the FeatureList object first."
        ),
    )
    return feature_list_namespace


@router.patch("/{feature_list_namespace_id}", response_model=FeatureListNamespaceModelResponse)
async def update_feature_list_namespace(
    request: Request, feature_list_namespace_id: PydanticObjectId, data: FeatureListNamespaceUpdate
) -> FeatureListNamespaceModelResponse:
    """
    Update FeatureListNamespace
    """
    controller = request.state.app_container.feature_list_namespace_controller
    feature_list_namespace: FeatureListNamespaceModelResponse = (
        await controller.update_feature_list_namespace(
            feature_list_namespace_id=feature_list_namespace_id,
            data=data,
        )
    )
    return feature_list_namespace


@router.get("", response_model=FeatureListNamespaceList)
async def list_feature_list_namespace(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> FeatureListNamespaceList:
    """
    List FeatureListNamespaces
    """
    controller = request.state.app_container.feature_list_namespace_controller
    feature_list_paginated_list: FeatureListNamespaceList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
    )
    return feature_list_paginated_list


@router.get("/audit/{feature_list_namespace_id}", response_model=AuditDocumentList)
async def list_feature_list_namespace_audit_logs(
    request: Request,
    feature_list_namespace_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List FeatureListNamespace audit logs
    """
    controller = request.state.app_container.feature_list_namespace_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_list_namespace_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_list_namespace_id}/info", response_model=FeatureListNamespaceInfo)
async def get_feature_list_namespace_info(
    request: Request,
    feature_list_namespace_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureListNamespaceInfo:
    """
    Retrieve FeatureListNamespace info
    """
    controller = request.state.app_container.feature_list_namespace_controller
    info = await controller.get_info(
        document_id=feature_list_namespace_id,
        verbose=verbose,
    )
    return cast(FeatureListNamespaceInfo, info)


@router.patch(
    "/{feature_list_namespace_id}/description", response_model=FeatureListNamespaceModelResponse
)
async def update_feature_list_namespace_description(
    request: Request,
    feature_list_namespace_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> FeatureListNamespaceModelResponse:
    """
    Update feature_list_namespace description
    """
    controller = request.state.app_container.feature_list_namespace_controller
    feature_list_namespace: FeatureListNamespaceModelResponse = await controller.update_description(
        document_id=feature_list_namespace_id,
        description=data.description,
    )
    return feature_list_namespace
