"""
FeatureList API routes
"""

from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any, Dict, Optional, Union, cast

from fastapi import APIRouter, File, Form, Query, Request, Response, UploadFile

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
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListCreateJob,
    FeatureListCreateWithBatchFeatureCreation,
    FeatureListGetHistoricalFeatures,
    FeatureListModelResponse,
    FeatureListNewVersionCreate,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListSQL,
    FeatureListUpdate,
    SampleEntityServingNames,
)
from featurebyte.schema.info import FeatureListInfo
from featurebyte.schema.task import Task

router = APIRouter(prefix="/feature_list")


class FeatureListRouter(BaseRouter):
    """
    Feature list router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=FeatureListModelResponse, status_code=HTTPStatus.CREATED)
async def create_feature_list(
    request: Request, data: Union[FeatureListCreate, FeatureListNewVersionCreate]
) -> FeatureListModelResponse:
    """
    Create FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    feature_list: FeatureListModelResponse = await controller.create_feature_list(data=data)
    return feature_list


@router.post("/batch", response_model=Task, status_code=HTTPStatus.CREATED)
async def submit_feature_create_with_batch_feature_create_task(
    request: Request, data: FeatureListCreateWithBatchFeatureCreation
) -> Task:
    """
    Submit FeatureList create with batch feature create task
    """
    # TO BE DEPRECATED: Use /job instead
    # This endpoint is for backward compatibility
    controller = request.state.app_container.feature_list_controller
    task: Task = await controller.submit_feature_list_create_with_batch_feature_create_task(
        data=data
    )
    return task


@router.post("/job", response_model=Task, status_code=HTTPStatus.CREATED)
async def submit_feature_list_creation_job(request: Request, data: FeatureListCreateJob) -> Task:
    """
    Submit Feature List creation job
    """
    controller = request.state.app_container.feature_list_controller
    task: Task = await controller.submit_feature_list_create_job(data=data)
    return task


@router.get("/{feature_list_id}", response_model=FeatureListModelResponse)
async def get_feature_list(
    request: Request, feature_list_id: PydanticObjectId
) -> FeatureListModelResponse:
    """
    Get FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    feature_list: FeatureListModelResponse = await controller.get(document_id=feature_list_id)
    return feature_list


@router.patch("/{feature_list_id}", response_model=Union[FeatureListModelResponse, Task])
async def update_feature_list(
    request: Request, feature_list_id: PydanticObjectId, data: FeatureListUpdate, response: Response
) -> Union[FeatureListModelResponse, Task]:
    """
    Update FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    feature_list_or_task = await controller.update_feature_list(
        feature_list_id=feature_list_id,
        data=data,
    )
    if isinstance(feature_list_or_task, Task):
        response.status_code = HTTPStatus.ACCEPTED
        return feature_list_or_task
    return cast(FeatureListModelResponse, feature_list_or_task)


@router.delete("/{feature_list_id}", status_code=HTTPStatus.OK)
async def delete_feature_list(
    request: Request, feature_list_id: PydanticObjectId
) -> DeleteResponse:
    """
    Delete FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    await controller.delete_feature_list(feature_list_id=feature_list_id)
    return DeleteResponse()


@router.get("", response_model=FeatureListPaginatedList)
async def list_feature_list(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
    version: Optional[str] = VersionQuery,
    feature_list_namespace_id: Optional[PydanticObjectId] = None,
) -> FeatureListPaginatedList:
    """
    List FeatureLists
    """
    controller = request.state.app_container.feature_list_controller
    feature_list_paginated_list: FeatureListPaginatedList = await controller.list_feature_lists(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
        version=version,
        feature_list_namespace_id=feature_list_namespace_id,
    )
    return feature_list_paginated_list


@router.get("/audit/{feature_list_id}", response_model=AuditDocumentList)
async def list_feature_list_audit_logs(
    request: Request,
    feature_list_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List FeatureList audit logs
    """
    controller = request.state.app_container.feature_list_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=feature_list_id,
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{feature_list_id}/info", response_model=FeatureListInfo)
async def get_feature_list_info(
    request: Request,
    feature_list_id: PydanticObjectId,
    verbose: bool = VerboseQuery,
) -> FeatureListInfo:
    """
    Retrieve FeatureList info
    """
    controller = request.state.app_container.feature_list_controller
    info = await controller.get_info(
        document_id=feature_list_id,
        verbose=verbose,
    )
    return cast(FeatureListInfo, info)


@router.post("/preview", response_model=Dict[str, Any])
async def get_feature_list_preview(
    request: Request,
    featurelist_preview: FeatureListPreview,
) -> Dict[str, Any]:
    """
    Retrieve Feature preview
    """
    controller = request.state.app_container.feature_list_controller
    return cast(
        Dict[str, Any],
        await controller.preview(featurelist_preview=featurelist_preview),
    )


@router.post("/sql", response_model=str)
async def get_feature_list_sql(
    request: Request,
    featurelist_sql: FeatureListSQL,
) -> str:
    """
    Retrieve FeatureList SQL
    """
    controller = request.state.app_container.feature_list_controller
    return cast(
        str,
        await controller.sql(featurelist_sql=featurelist_sql),
    )


@router.post("/historical_features_sql", response_model=str)
async def get_historical_features_sql(
    request: Request,
    payload: str = Form(),
    observation_set: UploadFile = File(description="Observation set data in parquet format"),
) -> str:
    """
    Retrieve historical features SQL
    """
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(**json.loads(payload))
    controller = request.state.app_container.feature_list_controller
    return cast(
        str,
        await controller.get_historical_features_sql(
            observation_set=observation_set,
            featurelist_get_historical_features=featurelist_get_historical_features,
        ),
    )


@router.get("/{feature_list_id}/feature_job_logs", response_model=Dict[str, Any])
async def get_feature_job_logs(
    request: Request,
    feature_list_id: PydanticObjectId,
    hour_limit: int = Query(default=24, gt=0, le=2400),
) -> Dict[str, Any]:
    """
    Retrieve feature job status
    """
    controller = request.state.app_container.feature_list_controller
    result = await controller.get_feature_job_logs(
        feature_list_id=feature_list_id,
        hour_limit=hour_limit,
    )
    return cast(Dict[str, Any], result)


@router.patch("/{feature_list_id}/description", response_model=FeatureListModelResponse)
async def update_feature_list_description(
    request: Request,
    feature_list_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> FeatureListModelResponse:
    """
    Update feature_list description
    """
    controller = request.state.app_container.feature_list_controller
    feature_list: FeatureListModelResponse = await controller.update_description(
        document_id=feature_list_id,
        description=data.description,
    )
    return feature_list


@router.get(
    "/{feature_list_id}/sample_entity_serving_names",
    response_model=SampleEntityServingNames,
)
async def get_feature_list_sample_entity_serving_names(
    request: Request,
    feature_list_id: PydanticObjectId,
    count: int = Query(default=1, gt=0, le=10),
) -> SampleEntityServingNames:
    """
    Get Feature List Sample Entity Serving Names
    """
    controller = request.state.app_container.feature_list_controller
    sample_entity_serving_names: SampleEntityServingNames = (
        await controller.get_sample_entity_serving_names(
            feature_list_id=feature_list_id, count=count
        )
    )
    return sample_entity_serving_names
