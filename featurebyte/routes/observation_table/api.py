"""
ObservationTable API routes
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request

from featurebyte.models.base import PydanticObjectId
from featurebyte.models.observation_table import ObservationTableModel
from featurebyte.routes.common.schema import (
    NameQuery,
    PageQuery,
    PageSizeQuery,
    SearchQuery,
    SortByQuery,
    SortDirQuery,
)
from featurebyte.schema.observation_table import ObservationTableList

router = APIRouter(prefix="/observation_table")


@router.get("/{observation_table_id}", response_model=ObservationTableModel)
async def get_observation_table(
    request: Request, observation_table_id: PydanticObjectId
) -> ObservationTableModel:
    """
    Get ObservationTable
    """
    controller = request.state.app_container.observation_table_controller
    observation_table: ObservationTableModel = await controller.get(
        document_id=observation_table_id
    )
    return observation_table


@router.get("", response_model=ObservationTableList)
async def list_observation_tables(
    request: Request,
    page: int = PageQuery,
    page_size: int = PageSizeQuery,
    sort_by: Optional[str] = SortByQuery,
    sort_dir: Optional[str] = SortDirQuery,
    search: Optional[str] = SearchQuery,
    name: Optional[str] = NameQuery,
) -> ObservationTableList:
    """
    List ObservationTables
    """
    controller = request.state.app_container.observation_table_controller
    periodic_task_list: ObservationTableList = await controller.list(
        page=page,
        page_size=page_size,
        sort_by=sort_by,
        sort_dir=sort_dir,
        search=search,
        name=name,
    )
    return periodic_task_list
