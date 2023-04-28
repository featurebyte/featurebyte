"""
FeatureList API routes
"""
# pylint: disable=duplicate-code
from __future__ import annotations

from typing import Any, Dict, Optional, Union, cast

import json
from http import HTTPStatus

from fastapi import APIRouter, File, Form, Query, Request, UploadFile
from fastapi.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.feature_list import FeatureListModel
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
    VersionQuery,
)
from featurebyte.schema.common.base import DeleteResponse
from featurebyte.schema.feature_list import (
    FeatureListCreate,
    FeatureListGetHistoricalFeatures,
    FeatureListNewVersionCreate,
    FeatureListPaginatedList,
    FeatureListPreview,
    FeatureListSQL,
    FeatureListUpdate,
)
from featurebyte.schema.info import FeatureListInfo

router = APIRouter(prefix="/feature_list")


@router.post("", response_model=FeatureListModel, status_code=HTTPStatus.CREATED)
async def create_feature_list(
    request: Request, data: Union[FeatureListCreate, FeatureListNewVersionCreate]
) -> FeatureListModel:
    """
    Create FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    feature_list: FeatureListModel = await controller.create_feature_list(data=data)
    return feature_list


@router.get("/{feature_list_id}", response_model=FeatureListModel)
async def get_feature_list(request: Request, feature_list_id: PydanticObjectId) -> FeatureListModel:
    """
    Get FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    feature_list: FeatureListModel = await controller.get(document_id=feature_list_id)
    return feature_list


@router.patch("/{feature_list_id}", response_model=FeatureListModel)
async def update_feature_list(
    request: Request, feature_list_id: PydanticObjectId, data: FeatureListUpdate
) -> FeatureListModel:
    """
    Update FeatureList
    """
    controller = request.state.app_container.feature_list_controller
    feature_list: FeatureListModel = await controller.update_feature_list(
        feature_list_id=feature_list_id,
        data=data,
    )
    return feature_list


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
    sort_dir: Optional[str] = SortDirQuery,
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
        sort_by=sort_by,
        sort_dir=sort_dir,
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
    sort_dir: Optional[str] = SortDirQuery,
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
        sort_by=sort_by,
        sort_dir=sort_dir,
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
        await controller.preview(
            featurelist_preview=featurelist_preview, get_credential=request.state.get_credential
        ),
    )


@router.post("/historical_features")
async def get_historical_features(
    request: Request,
    payload: str = Form(),
    observation_set: UploadFile = File(description="Observation set data in parquet format"),
) -> StreamingResponse:
    """
    Retrieve historical features
    """
    featurelist_get_historical_features = FeatureListGetHistoricalFeatures(**json.loads(payload))
    controller = request.state.app_container.feature_list_controller
    result: StreamingResponse = await controller.compute_historical_features(
        observation_set=observation_set,
        featurelist_get_historical_features=featurelist_get_historical_features,
        get_credential=request.state.get_credential,
    )
    return result


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
        get_credential=request.state.get_credential,
    )
    return cast(Dict[str, Any], result)
