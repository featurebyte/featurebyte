"""
System metrics API routes
"""

from __future__ import annotations

from typing import Any, Optional

from fastapi import APIRouter, Query, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    PageQuery,
    PageSizeQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.routes.system_metrics.controller import SystemMetricsController
from featurebyte.schema.system_metrics import SystemMetricsList

router = APIRouter(prefix="/system_metrics")


class SystemMetricsRouter(BaseRouter):
    """
    System metrics router
    """

    def __init__(self) -> None:
        super().__init__(router=router)


@router.get("", response_model=SystemMetricsList)
async def list_metrics(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[SortDir] = SortDirQuery,
    metrics_type: Optional[str] = Query(default=None),
    historical_feature_table_id: Optional[PydanticObjectId] = Query(default=None),
    tile_table_id: Optional[str] = Query(default=None),
    offline_store_feature_table_id: Optional[PydanticObjectId] = Query(default=None),
    deployment_id: Optional[PydanticObjectId] = Query(default=None),
    observation_table_id: Optional[PydanticObjectId] = Query(default=None),
    batch_feature_table_id: Optional[PydanticObjectId] = Query(default=None),
) -> SystemMetricsList:
    """
    List system metrics
    """
    controller: SystemMetricsController = request.state.app_container.system_metrics_controller

    query_filter = {}

    def _add_metrics_data_filter(field: str, value: Any) -> None:
        query_filter[f"metrics_data.{field}"] = value

    if metrics_type is not None:
        _add_metrics_data_filter("metrics_type", metrics_type)

    if historical_feature_table_id is not None:
        _add_metrics_data_filter("historical_feature_table_id", historical_feature_table_id)

    if tile_table_id is not None:
        _add_metrics_data_filter("tile_table_id", tile_table_id)

    if offline_store_feature_table_id is not None:
        _add_metrics_data_filter("offline_store_feature_table_id", offline_store_feature_table_id)

    if deployment_id is not None:
        _add_metrics_data_filter("deployment_id", deployment_id)

    if observation_table_id is not None:
        _add_metrics_data_filter("observation_table_id", observation_table_id)

    if batch_feature_table_id is not None:
        _add_metrics_data_filter("batch_feature_table_id", batch_feature_table_id)

    system_metrics_list = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        query_filter=query_filter,
    )
    return system_metrics_list
