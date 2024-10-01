"""
System metrics API routes
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Query, Request

from featurebyte.persistent.base import SortDir
from featurebyte.routes.base_router import BaseRouter
from featurebyte.routes.common.schema import (
    NameQuery,
    PageQuery,
    PageSizeQuery,
    PyObjectId,
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
    type: Optional[str] = NameQuery,
    historical_feature_table_id: Optional[PyObjectId] = Query(default=None),
) -> SystemMetricsList:
    """
    List Table
    """
    controller: SystemMetricsController = request.state.app_container.system_metrics_controller

    query_filter = {}
    if type is not None:
        query_filter["metrics_data.type"] = type
    if historical_feature_table_id is not None:
        query_filter["metrics_data.historical_feature_table_id"] = historical_feature_table_id

    system_metrics_list = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=[(sort_by, sort_dir)] if sort_by and sort_dir else None,
        query_filter=query_filter,
    )
    return system_metrics_list
