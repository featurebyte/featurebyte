"""
HistoricalFeatureTable API routes
"""

from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any, Dict, Optional

from bson import ObjectId
from fastapi import APIRouter, Form, Query, Request, UploadFile
from starlette.responses import StreamingResponse

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.historical_feature_table import HistoricalFeatureTableModel
from featurebyte.models.persistent import AuditDocumentList
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    PREVIEW_DEFAULT,
    PREVIEW_LIMIT,
    AuditLogSortByQuery,
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
    VerboseQuery,
)
from featurebyte.routes.historical_feature_table.controller import HistoricalFeatureTableController
from featurebyte.schema.common.base import DescriptionUpdate
from featurebyte.schema.historical_feature_table import (
    HistoricalFeatureTableCreate,
    HistoricalFeatureTableList,
    HistoricalFeatureTableUpdate,
)
from featurebyte.schema.info import HistoricalFeatureTableInfo
from featurebyte.schema.task import Task

router = APIRouter(prefix="/historical_feature_table")


class HistoricalFeatureTableRouter(BaseRouter):
    """
    Historical feature table router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.post("", response_model=Task, status_code=HTTPStatus.CREATED)
async def create_historical_feature_table(
    request: Request,
    payload: str = Form(),
    observation_set: Optional[UploadFile] = None,
) -> Task:
    """
    Create HistoricalFeatureTable by submitting a materialization task
    """
    data = HistoricalFeatureTableCreate(**json.loads(payload))
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.create_table(
        data=data,
        observation_set=observation_set,
    )


@router.get("/{historical_feature_table_id}", response_model=HistoricalFeatureTableModel)
async def get_historical_feature_table(
    request: Request, historical_feature_table_id: PydanticObjectId
) -> HistoricalFeatureTableModel:
    """
    Get HistoricalFeatureTable
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.get(document_id=ObjectId(historical_feature_table_id))


@router.delete(
    "/{historical_feature_table_id}", response_model=Task, status_code=HTTPStatus.ACCEPTED
)
async def delete_historical_feature_table(
    request: Request, historical_feature_table_id: PydanticObjectId
) -> Task:
    """
    Delete HistoricalFeatureTable
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.delete_materialized_table(
        document_id=ObjectId(historical_feature_table_id)
    )


@router.get("", response_model=HistoricalFeatureTableList)
async def list_historical_feature_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> HistoricalFeatureTableList:
    """
    List HistoricalFeatureTables
    """
    controller = request.state.app_container.historical_feature_table_controller
    historical_feature_table_list: HistoricalFeatureTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
        name=name,
    )
    return historical_feature_table_list


@router.get("/audit/{historical_feature_table_id}", response_model=AuditDocumentList)
async def list_historical_feature_table_audit_logs(
    request: Request,
    historical_feature_table_id: PydanticObjectId,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = AuditLogSortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    search: Optional[str] = SearchQuery,
) -> AuditDocumentList:
    """
    List HistoricalFeatureTable audit logs
    """
    controller = request.state.app_container.historical_feature_table_controller
    audit_doc_list: AuditDocumentList = await controller.list_audit(
        document_id=ObjectId(historical_feature_table_id),
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        search=search,
    )
    return audit_doc_list


@router.get("/{historical_feature_table_id}/info", response_model=HistoricalFeatureTableInfo)
async def get_historical_feature_table_info(
    request: Request, historical_feature_table_id: PydanticObjectId, verbose: bool = VerboseQuery
) -> HistoricalFeatureTableInfo:
    """
    Get HistoricalFeatureTable info
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.get_info(
        document_id=ObjectId(historical_feature_table_id), verbose=verbose
    )


@router.get("/pyarrow_table/{historical_feature_table_id}")
async def download_table_as_pyarrow_table(
    request: Request, historical_feature_table_id: PydanticObjectId
) -> StreamingResponse:
    """
    Download HistoricalFeatureTable as pyarrow table
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.download_materialized_table(
        document_id=ObjectId(historical_feature_table_id)
    )


@router.get("/parquet/{historical_feature_table_id}")
async def download_table_as_parquet(
    request: Request, historical_feature_table_id: PydanticObjectId
) -> StreamingResponse:
    """
    Download HistoricalFeatureTable as parquet file
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.download_materialized_table_as_parquet(
        document_id=ObjectId(historical_feature_table_id),
    )


@router.patch(
    "/{historical_feature_table_id}/description", response_model=HistoricalFeatureTableModel
)
async def update_historical_feature_table_description(
    request: Request,
    historical_feature_table_id: PydanticObjectId,
    data: DescriptionUpdate,
) -> HistoricalFeatureTableModel:
    """
    Update historical_feature_table description
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    return await controller.update_description(
        document_id=ObjectId(historical_feature_table_id),
        description=data.description,
    )


@router.patch("/{historical_feature_table_id}", response_model=HistoricalFeatureTableModel)
async def update_historical_feature_table(
    request: Request,
    historical_feature_table_id: PydanticObjectId,
    data: HistoricalFeatureTableUpdate,
) -> HistoricalFeatureTableModel:
    """
    Update historical_feature_table
    """
    controller: HistoricalFeatureTableController = (
        request.state.app_container.historical_feature_table_controller
    )
    table = await controller.update_historical_feature_table(
        historical_feature_table_id=ObjectId(historical_feature_table_id),
        data=data,
    )
    return table


@router.post("/{historical_feature_table_id}/preview", response_model=Dict[str, Any])
async def preview_historical_feature_table(
    request: Request,
    historical_feature_table_id: PydanticObjectId,
    limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
) -> Dict[str, Any]:
    """
    Preview historical feature table
    """
    controller = request.state.app_container.historical_feature_table_controller
    preview: Dict[str, Any] = await controller.preview_materialized_table(
        document_id=historical_feature_table_id,
        limit=limit,
    )
    return preview


@router.get(
    "/{historical_feature_table_id}/feature_preview/{feature_id}", response_model=Dict[str, Any]
)
async def preview_historical_feature_table_feature(
    request: Request,
    historical_feature_table_id: PydanticObjectId,
    feature_id: PydanticObjectId,
    limit: int = Query(default=PREVIEW_DEFAULT, gt=0, le=PREVIEW_LIMIT),
) -> Dict[str, Any]:
    """
    Preview feature in historical feature table
    """
    controller = request.state.app_container.historical_feature_table_controller
    preview: Dict[str, Any] = await controller.preview_feature(
        document_id=historical_feature_table_id,
        feature_id=feature_id,
        limit=limit,
    )
    return preview
