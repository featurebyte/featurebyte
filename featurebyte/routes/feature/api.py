"""
Feature API routes
"""

from __future__ import annotations

from http import HTTPStatus
from typing import Any, Dict, Optional, Union, cast

from fastapi import APIRouter, Query, Request

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
    VersionQuery,
)
from featurebyte.schema.common.base import DeleteResponse, DescriptionUpdate
from featurebyte.schema.feature import (
    BatchFeatureCreate,
    FeatureCreate,
    FeatureModelResponse,
    FeatureNewVersionCreate,
    FeaturePaginatedList,
    FeatureSQL,
    FeatureUpdate,
)
from featurebyte.schema.feature_list import SampleEntityServingNames
from featurebyte.schema.info import FeatureInfo
from featurebyte.schema.preview import FeaturePreview
from featurebyte.schema.task import Task

router = APIRouter(prefix="/feature")


class FeatureRouter(BaseRouter):
    """
    Feature router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=FeatureModelResponse, status_code=HTTPStatus.CREATED)
async def create_feature(
    request: Request, data: Union[FeatureCreate, FeatureNewVersionCreate]
) -> FeatureModelResponse:
    """
    Create Feature
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModelResponse = await controller.create_feature(data=data)
    return feature


@router.post("/batch", response_model=Task, status_code=HTTPStatus.CREATED)
async def submit_batch_feature_create_task(request: Request, data: BatchFeatureCreate) -> Task:
    """
    Submit Batch Feature Create Task
    """
    controller = request.state.app_container.feature_controller
    task: Task = await controller.submit_batch_feature_create_task(data=data)
    return task


@router.get("/{feature_id}", response_model=FeatureModelResponse)
async def get_feature(request: Request, feature_id: PydanticObjectId) -> FeatureModelResponse:
    """
    Get Feature
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModelResponse = await controller.get(document_id=feature_id)
    return feature


@router.patch("/{feature_id}", response_model=FeatureModelResponse)
async def update_feature(
    request: Request, feature_id: PydanticObjectId, data: FeatureUpdate
) -> FeatureModelResponse:
    """
    Update Feature
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModelResponse = await controller.update_feature(
        feature_id=feature_id,
        data=data,
    )
    return feature


@router.delete("/{feature_id}", status_code=HTTPStatus.OK)
async def delete_feature(request: Request, feature_id: PydanticObjectId) -> DeleteResponse:
    """
    Delete Feature
    """
    controller = request.state.app_container.feature_controller
    await controller.delete_feature(feature_id=feature_id)
    return DeleteResponse()


@router.get("", response_model=FeaturePaginatedList)
async def list_features(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    version: Optional[str] = VersionQuery,
    feature_list_id: Optional[PydanticObjectId] = None,
    feature_namespace_id: Optional[PydanticObjectId] = None,
) -> FeaturePaginatedList:
    """
    List Features
    """
    controller = request.state.app_container.feature_controller
    feature_list: FeaturePaginatedList = await controller.list_features(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
        version=version,
        feature_list_id=feature_list_id,
        feature_namespace_id=feature_namespace_id,
    )
    return feature_list


@router.get("/audit/{feature_id}", response_model=AuditDocumentList)
async def list_feature_audit_logs(
    request: Request,
    feature_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List Feature audit logs
    """
    controller = request.state.app_container.feature_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_id}/info", response_model=FeatureInfo)
async def get_feature_info(
    request: Request,
    feature_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureInfo:
    """
    Retrieve Feature info
    """
    controller = request.state.app_container.feature_controller
    info = await controller.get_info(
        document_id=feature_id,
        verbose=verbose,
    )
    return cast(FeatureInfo, info)


@router.post("/preview", response_model=Dict[str, Any])
async def get_feature_preview(
    request: Request,
    feature_preview: FeaturePreview,
) -> Dict[str, Any]:
    """
    Retrieve Feature preview
    """
    controller = request.state.app_container.feature_controller
    return cast(
        Dict[str, Any],
        await controller.preview(feature_preview=feature_preview),
    )


@router.post("/sql", response_model=str)
async def get_feature_sql(
    request: Request,
    feature_sql: FeatureSQL,
) -> str:
    """
    Retrieve Feature SQL
    """
    controller = request.state.app_container.feature_controller
    return cast(
        str,
        await controller.sql(feature_sql=feature_sql),
    )


@router.get("/{feature_id}/feature_job_logs", response_model=Dict[str, Any])
async def get_feature_job_logs(
    request: Request,
    feature_id: PydanticObjectId,
    hour_limit: int = Query(default=24, gt=0, le=2400),
) -> Dict[str, Any]:
    """
    Retrieve feature job status
    """
    controller = request.state.app_container.feature_controller
    result = await controller.get_feature_job_logs(
        feature_id=feature_id,
        hour_limit=hour_limit,
    )
    return cast(Dict[str, Any], result)


@router.patch("/{feature_id}/description", response_model=FeatureModelResponse)
async def update_feature_description(
    request: Request,
    feature_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> FeatureModelResponse:
    """
    Update feature description
    """
    controller = request.state.app_container.feature_controller
    feature: FeatureModelResponse = await controller.update_description(
        document_id=feature_id,
        description=data.description,
    )
    return feature


@router.get(
    "/{feature_id}/sample_entity_serving_names",
    response_model=SampleEntityServingNames,
)
async def get_feature_sample_entity_serving_names(
    request: Request,
    feature_id: PydanticObjectId,
    count: int = Query(default=1, gt=0, le=10),
) -> SampleEntityServingNames:
    """
    Get Feature Sample Entity Serving Names
    """
    controller = request.state.app_container.feature_controller
    sample_entity_serving_names: SampleEntityServingNames = (
        await controller.get_sample_entity_serving_names(feature_id=feature_id, count=count)
    )
    return sample_entity_serving_names
